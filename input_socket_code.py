import socket
import time

host_name = "localhost"  # The server's address
port_num = 9999  # Port to listen on
input_file_path = "sentences.txt"  # Path to the file to be streamed
stream_interval = 10  # Time delay (in seconds) between each line sent

# Setting up server socket
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((host_name, port_num))
server_socket.listen(1)  # Allow only one client connection at a time

# Waiting for a client connection
client_connection, address = server_socket.accept()

# Opening the file and streaming its contents to the client
with open(input_file_path, "r") as file_stream:
    # Process each line from the file
    for text_line in file_stream:
        # Prepare the line for transmission (encode to bytes)
        line_data = text_line.encode("utf-8")
        
        # Send the line to the connected client
        client_connection.sendall(line_data)
        print(f"Sent: {text_line.strip()}")
        # Introduce a delay between transmissions to simulate a real-time stream
        time.sleep(stream_interval)

# Close the connections after the streaming is complete
client_connection.close()
server_socket.close()