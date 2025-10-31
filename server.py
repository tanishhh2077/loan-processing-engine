# server.py
import os
import time
import traceback
from concurrent import futures
from collections import Counter
from urllib.parse import urlparse

import grpc
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import mysql.connector
from mysql.connector import Error as MySQLError

import lender_pb2
import lender_pb2_grpc


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_DB   = os.getenv("MYSQL_DB", "CS544")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PW   = os.getenv("MYSQL_PASSWORD", "abc")

HDFS_NN_HOST = os.getenv("HDFS_NN_HOST", "nn")
HDFS_NN_PORT = 9000
HDFS_WEB_PORT = int(os.getenv("HDFS_WEB_PORT", "9870"))

HADOOP_HOME = os.getenv("HADOOP_HOME")
if HADOOP_HOME:
    os.environ["CLASSPATH"] = f"{HADOOP_HOME}/bin/hdfs"

PARQUET_PATH = "/hdma-wi-2021.parquet"
BLOCK_SIZE   = 1 * 1024 * 1024     # 1 MB
REPLICATION  = 2


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def mysql_connect_with_retry(max_wait_s=180):
    start = time.time()
    last_err = None
    while time.time() - start < max_wait_s:
        try:
            conn = mysql.connector.connect(
                host=MYSQL_HOST, database=MYSQL_DB,
                user=MYSQL_USER, password=MYSQL_PW
            )
            if conn.is_connected():
                print("Connected to MySQL")
                return conn
        except MySQLError as e:
            last_err = e
            print("Waiting for MySQL to be ready...")
            time.sleep(3)
    raise RuntimeError(f"MySQL not ready after {max_wait_s}s: {last_err}")


def fetch_filtered_dataframe():
    """Join and filter loans with loan_types in MySQL."""
    SQL = """
     SELECT
        *
    FROM loans l
    JOIN loan_types lt ON l.loan_type_id = lt.id
    WHERE l.loan_amount BETWEEN 30000 AND 800000
    """
    conn = mysql_connect_with_retry()
    try:
        df = pd.read_sql(SQL, conn)
        print(f"Loaded {len(df)} rows from MySQL")
        return df
    finally:
        conn.close()

def write_parquet_to_hdfs(df: pd.DataFrame, path: str):
    """Write dataframe to HDFS using WebHDFS API."""
    # Convert to parquet in memory
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf)
    parquet_bytes = buf.getvalue().to_pybytes()
    
    # Construct WebHDFS URL for CREATE operation
    if not path.startswith('/'):
        path = '/' + path
    
    # Step 1: CREATE request (gets redirect)
    create_url = f"http://{HDFS_NN_HOST}:{HDFS_WEB_PORT}/webhdfs/v1{path}?op=CREATE&user.name=root&overwrite=true&replication={REPLICATION}&blocksize={BLOCK_SIZE}"
    
    response = requests.put(create_url, allow_redirects=False)
    if response.status_code != 307:
        raise RuntimeError(f"WebHDFS CREATE failed: {response.text}")
    
    # Step 2: Follow redirect to DataNode
    datanode_url = response.headers['Location']
    try:
        response = requests.put(datanode_url, data=parquet_bytes)
    except requests.exceptions.ConnectionError:
        print("Connection to DataNode failed, retrying via NameNode proxy.")
        parsed_url = urlparse(datanode_url)
        retry_url = f"http://{HDFS_NN_HOST}:{HDFS_WEB_PORT}{parsed_url.path}?{parsed_url.query}"
        print(f"Retrying PUT to: {retry_url}")
        response = requests.put(retry_url, data=parquet_bytes)
    response.raise_for_status()
    
    print(f"Wrote {len(parquet_bytes)/1e6:.2f} MB to {path}")
    return len(parquet_bytes), df.shape[0]



# gRPC implementation
# ---------------------------------------------------------------------
class LenderServicer(lender_pb2_grpc.LenderServicer):

    def DbToHdfs(self, request, context):
        print("Received DbToHdfs RPC")
        try:
            df = fetch_filtered_dataframe()
            if df.empty:
                msg = "No data matched filter; nothing written."
                print(msg)
                return lender_pb2.StatusString(status=msg)

            size_bytes, rows = write_parquet_to_hdfs(df, PARQUET_PATH)

            # Verify file exists by checking with WebHDFS
            verify_url = f"http://{HDFS_NN_HOST}:{HDFS_WEB_PORT}/webhdfs/v1{PARQUET_PATH}?op=GETFILESTATUS&user.name=root"
            verify_response = requests.get(verify_url, timeout=10)

            if verify_response.status_code != 200:
                err_msg = f"File written but verification failed: {verify_response.text}"
                print(err_msg)
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(err_msg)
                return lender_pb2.StatusString(status=err_msg)

            msg = f"DbToHdfs completed: {rows} rows → {PARQUET_PATH} ({size_bytes/1e6:.2f} MB)"
            print(msg)
            return lender_pb2.StatusString(status=msg)

        except Exception as e:
            tb = traceback.format_exc()
            err = f"DbToHdfs failed: {e}\n{tb}"
            print(err)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return lender_pb2.StatusString(status=err)

    def BlockLocations(self, request, context):
        print(f"Received BlockLocations RPC for path: {request.path}")
        try:
            # Construct WebHDFS URL
            path = request.path
            if not path.startswith('/'):
                path = '/' + path
            url = f"http://{HDFS_NN_HOST}:{HDFS_WEB_PORT}/webhdfs/v1{path}?op=GETFILEBLOCKLOCATIONS"

            # Make request to NameNode
            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            # Validate JSON structure
            block_locations_data = data.get('BlockLocations')
            if not isinstance(block_locations_data, dict):
                err_msg = f"Invalid JSON response from WebHDFS: 'BlockLocations' key is missing or not an object."
                print(err_msg)
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(err_msg)
                return lender_pb2.BlockLocationsResp(error=err_msg)

            # The key is 'BlockLocation' (array of blocks)
            block_list = block_locations_data.get('BlockLocation', [])
            if not isinstance(block_list, list):
                err_msg = f"Invalid JSON response from WebHDFS: 'BlockLocation' key is missing or not a list."
                print(err_msg)
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(err_msg)
                return lender_pb2.BlockLocationsResp(error=err_msg)

            if not block_list:
                msg = "No block locations found for this file."
                print(msg)
                return lender_pb2.BlockLocationsResp(error=msg)

            # Count blocks per host
            host_counts = Counter()
            for block in block_list:
                # Use 'hosts' field which contains hostnames
                hosts = block.get('hosts', [])
                for host in hosts:
                    host_counts[host] += 1

            print(f"Block counts: {dict(host_counts)}")
            return lender_pb2.BlockLocationsResp(block_entries=host_counts)

        except requests.exceptions.HTTPError as e:
            err_msg = f"HTTP error from WebHDFS: {e}"
            try:
                error_json = e.response.json()
                if 'RemoteException' in error_json and 'message' in error_json['RemoteException']:
                    err_msg = f"WebHDFS error: {error_json['RemoteException']['message']}"
            except Exception:
                pass
            print(err_msg)
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(err_msg)
            return lender_pb2.BlockLocationsResp(error=err_msg)

        except requests.exceptions.RequestException as e:
            err_msg = f"Failed to connect to HDFS NameNode via WebHDFS: {e}"
            print(err_msg)
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(err_msg)
            return lender_pb2.BlockLocationsResp(error=err_msg)

        except Exception as e:
            tb = traceback.format_exc()
            err = f"BlockLocations failed: {e}\n{tb}"
            print(err)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return lender_pb2.BlockLocationsResp(error=err)

    def CalcAvgLoan(self, request, context):
        """
        Calculates the average loan amount for a given county.

        This method demonstrates the benefit of data partitioning. On the first
        request for a county, it reads the full dataset, filters it, calculates
        the average, and saves the filtered data as a separate "partition" file
        in HDFS.

        Subsequent requests for the same county will read the much smaller
        partition file directly. This is significantly faster because it:
        1. Reduces I/O: Less data is read from HDFS.
        2. Avoids Computation: The expensive filtering of the full dataset is
           skipped.
        """
        print(f"Received CalcAvgLoan RPC for county_code: {request.county_code}")
        try:
            county_code = request.county_code
            partition_dir = "/partitions"
            partition_path = f"{partition_dir}/{county_code}.parquet"
            source = ""

            # Check if partition exists using WebHDFS
            partition_status_url = f"http://{HDFS_NN_HOST}:{HDFS_WEB_PORT}/webhdfs/v1{partition_path}?op=GETFILESTATUS&user.name=root"
            response = requests.get(partition_status_url)

            table = None
            if response.status_code == 200:
                print(f"Partition found at {partition_path}, attempting to reuse.")
                try:
                    open_url = f"http://{HDFS_NN_HOST}:{HDFS_WEB_PORT}/webhdfs/v1{partition_path}?op=OPEN&user.name=root"
                    read_response = requests.get(open_url, allow_redirects=True)
                    read_response.raise_for_status()
                    
                    parquet_bytes = read_response.content
                    table = pq.read_table(pa.BufferReader(parquet_bytes))
                    source = "reuse"
                except (requests.exceptions.RequestException, OSError) as e:
                    print(f"Failed to read existing partition {partition_path} due to: {e}. Recreating.")
                    source = "recreate"
                    # table is None, will proceed to creation logic below

            if response.status_code == 404 or source == "recreate":
                if source != "recreate":
                    print(f"Partition not found, creating from {PARQUET_PATH}.")
                    source = "create"

                # Check if base file exists.
                base_file_status_url = f"http://{HDFS_NN_HOST}:{HDFS_WEB_PORT}/webhdfs/v1{PARQUET_PATH}?op=GETFILESTATUS&user.name=root"
                base_file_response = requests.get(base_file_status_url)
                if base_file_response.status_code == 404:
                    err_msg = f"Base Parquet file not found at {PARQUET_PATH}. Run DbToHdfs first."
                    print(err_msg)
                    context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                    context.set_details(err_msg)
                    return lender_pb2.CalcAvgLoanResp(error=err_msg)
                base_file_response.raise_for_status()
                
                # Read base file and filter in-memory.
                open_url = f"http://{HDFS_NN_HOST}:{HDFS_WEB_PORT}/webhdfs/v1{PARQUET_PATH}?op=OPEN&user.name=root"
                read_response = requests.get(open_url, allow_redirects=True)
                read_response.raise_for_status()
            
                base_table = pq.read_table(pa.BufferReader(read_response.content))
                
                # Filter in-memory using pyarrow compute functions
                mask = pa.compute.equal(base_table['county_code'], county_code)
                table = base_table.filter(mask)
                
                # Ensure partitions directory exists.
                mkdirs_url = f"http://{HDFS_NN_HOST}:{HDFS_WEB_PORT}/webhdfs/v1{partition_dir}?op=MKDIRS&user.name=root"
                mkdirs_response = requests.put(mkdirs_url)
                mkdirs_response.raise_for_status()
                
                # Write partition to HDFS via WebHDFS.
                buf = pa.BufferOutputStream()
                pq.write_table(table, buf)
                partition_bytes = buf.getvalue().to_pybytes()

                create_url = f"http://{HDFS_NN_HOST}:{HDFS_WEB_PORT}/webhdfs/v1{partition_path}?op=CREATE&user.name=root&overwrite=true&replication=1"
                create_response = requests.put(create_url, allow_redirects=False)
                if create_response.status_code != 307:
                    raise RuntimeError(f"WebHDFS CREATE for partition failed: {create_response.text}")
                
                datanode_url = create_response.headers['Location']
                try:
                    write_response = requests.put(datanode_url, data=partition_bytes)
                except requests.exceptions.ConnectionError:
                    print("Connection to DataNode failed, retrying via NameNode proxy.")
                    parsed_url = urlparse(datanode_url)
                    retry_url = f"http://{HDFS_NN_HOST}:{HDFS_WEB_PORT}{parsed_url.path}?{parsed_url.query}"
                    print(f"Retrying PUT to: {retry_url}")
                    write_response = requests.put(retry_url, data=partition_bytes)
                write_response.raise_for_status()

                if table.num_rows > 0:
                    print(f"Filtered table has {table.num_rows} rows. Wrote partition to {partition_path}")
                else:
                    print(f"No loans found for county_code {county_code}. Wrote empty partition to {partition_path}.")
            
            elif table is None:
                err_msg = f"Unexpected status from WebHDFS GETFILESTATUS on {partition_path}: {response.status_code} {response.text}"
                print(err_msg)
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(err_msg)
                return lender_pb2.CalcAvgLoanResp(error=err_msg)

            if table.num_rows == 0:
                return lender_pb2.CalcAvgLoanResp(avg_loan=0, source=source)

            df = table.to_pandas()
            avg_loan = int(df['loan_amount'].mean())

            print(f"Calculated avg_loan: {avg_loan} from {table.num_rows} loans. Source: {source}")
            return lender_pb2.CalcAvgLoanResp(avg_loan=avg_loan, source=source)

        except requests.exceptions.HTTPError as e:
            err_msg = f"HTTP error from WebHDFS: {e}"
            try:
                error_json = e.response.json()
                if 'RemoteException' in error_json and 'message' in error_json['RemoteException']:
                    err_msg = f"WebHDFS error: {error_json['RemoteException']['message']}"
            except Exception:
                pass
            print(err_msg)
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(err_msg)
            return lender_pb2.CalcAvgLoanResp(error=err_msg)

        except requests.exceptions.RequestException as e:
            err_msg = f"Failed to connect to HDFS NameNode via WebHDFS: {e}"
            print(err_msg)
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(err_msg)
            return lender_pb2.CalcAvgLoanResp(error=err_msg)

        except Exception as e:
            tb = traceback.format_exc()
            err = f"CalcAvgLoan failed: {e}\n{tb}"
            print(err)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return lender_pb2.CalcAvgLoanResp(error=err)
# ---------------------------------------------------------------------
# Server bootstrap
# ---------------------------------------------------------------------
def serve():
    """Starts the gRPC server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    lender_pb2_grpc.add_LenderServicer_to_server(LenderServicer(), server)
    server.add_insecure_port('[::]:5000')  
    server.start()
    print("Server started on port 5000. Awaiting termination.")
    server.wait_for_termination()



if __name__ == "__main__":
    serve()
