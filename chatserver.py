import socket
import threading
import sys
import time
import queue
import os


class Client:
    def __init__(self, username, connection, address):
        self.username = username
        self.connection = connection
        self.address = address
        self.kicked = False
        self.in_queue = True
        self.remaining_time = 100 # remaining time before AFK
        self.muted = False
        self.mute_duration = 0


class Channel:
    def __init__(self, name, port, capacity):
        self.name = name
        self.port = port
        self.capacity = capacity
        self.queue = queue.Queue()
        self.clients = []

def parse_config(config_file: str) -> list:
    """
    Parses lines from a given configuration file and validates the format of each line.
    Ensures no duplicate channel names or ports and adheres to the specified rules.
    Args:
        config_file (str): The path to the configuration file (e.g., configs2.txt).
    Returns:
        list: A list of tuples where each tuple contains (channel_name, channel_port, channel_capacity).
    Raises:
        SystemExit: If there is an error in the configuration file format.
    """
    try:
        with open(config_file, 'r') as file:
            channels = {}
            ports = set()
            for line in file:
                line = line.strip()
                if not line or line.startswith('#'):  # Ignore empty lines and comments
                    continue
                parts = line.split()
                if len(parts) != 4 or parts[0] != 'channel':
                    raise ValueError("Line format is incorrect.")
                
                channel_name, channel_port, channel_capacity = parts[1], int(parts[2]), int(parts[3])
                
                if any(char.isdigit() or not char.isalnum() for char in channel_name):
                    raise ValueError("Channel name contains numbers or special characters.")
                
                if channel_port in ports:
                    raise ValueError("Duplicate channel port found.")
                
                if channel_capacity < 1 or channel_capacity > 5:
                    raise ValueError("Channel capacity must be between 1 and 5.")
                
                channels[channel_name] = (channel_name, channel_port, channel_capacity)
                ports.add(channel_port)
            
            if len(channels) < 1:
                raise ValueError("At least one channel is required in the configuration file.")
            
            return list(channels.values())
    except Exception as e:
        print(f"Error parsing configuration file: {e}")
        sys.exit(1)



def get_channels_dictionary(parsed_lines) -> dict:
    """
    Creates a dictionary of Channel objects from parsed lines.
    Status: Given
    Args:
        parsed_lines (list): A list of tuples where each tuple contains:
        (channel_name, channel_port, and channel_capacity)
    Returns:
        dict: A dictionary of Channel objects where the key is the channel name.
    """
    channels = {}

    for channel_name, channel_port, channel_capacity in parsed_lines:
        channels[channel_name] = Channel(channel_name, channel_port, channel_capacity)

    return channels

def quit_client(client, channel) -> None:
    """
    Implement client quitting function
    """
    quit_msg = f"[Server message ({time.strftime('%H:%M:%S')})] {client.username} has left the channel.\n"

    # Remove the client from the queue or channel
    if client.in_queue:
        channel.queue = remove_item(channel.queue, client)
    else:
        if client in channel.clients:
            channel.clients.remove(client)

    # Log the quit message on the server
    print(quit_msg.strip())

    # Broadcast the quit message to remaining clients
    for other_client in channel.clients:
        other_client.connection.send(quit_msg.encode())

    # Close client connection
    client.connection.close()

def send_client(client, channel, msg):
    """
    Handles the '/send' command from a client to send a file to another client in the same channel.
    """
    if client.in_queue:
        client.connection.send("You cannot send files from the waiting queue.\n".encode())
        return

    if client.muted:
        client.connection.send("You are currently muted and cannot send files.\n".encode())
        return

    parts = msg.split(maxsplit=3)
    if len(parts) < 3:
        client.connection.send("Usage: /send <target> <file_path>\n".encode())
        return

    _, target_username, file_path = parts

    # Check if the target user is in the same channel
    target_client = next((x for x in channel.clients if x.username == target_username), None)
    if not target_client:
        client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] {target_username} is not here.\n".encode())
        return

    # Check if the file exists
    if not os.path.exists(file_path):
        client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] {file_path} does not exist.\n".encode())
        return

    # Confirm to the sender that the file was "sent"
    client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] You sent {file_path} to {target_username}.\n".encode())
    print(f"[Server message ({time.strftime('%H:%M:%S')})] {client.username} sent {file_path} to {target_username}.")

def list_clients(client, channels):
    """
    Sends the list of all channels with their capacities and queue lengths to the client.
    """
    message_lines = []
    for channel_name, channel_info in channels.items():
        # Assuming channel_info is a tuple or a class with attributes
        channel_port, channel_capacity = channel_info.port, channel_info.capacity
        current = len([c for c in channel_info.clients if not c.in_queue])
        in_queue = len([c for c in channel_info.clients if c.in_queue])
        
        message = f"[Channel] {channel_name} {channel_port} Capacity: {current}/ {channel_capacity}, Queue: {in_queue}."
        message_lines.append(message)
    
    client.connection.send("\n".join(message_lines).encode())

def whisper_client(client, channel, msg):
    """
    Handles the '/whisper' command to send a private message to another client in the same channel.
    """
    if client.in_queue:
        client.connection.send("You cannot whisper from the waiting queue.\n".encode())
        return
    
    if client.muted:
        client.connection.send(f"You are currently muted and cannot whisper.\n".encode())
        return

    # Split the command to get the target username and the message
    parts = msg.split(maxsplit=2)
    if len(parts) < 3:
        client.connection.send("Usage: /whisper <username> <message>\n".encode())
        return

    _, target_username, whisper_message = parts

    # Find the target client in the same channel
    target_client = next((x for x in channel.clients if x.username == target_username), None)
    if not target_client:
        client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] {target_username} is not here.\n".encode())
        return

    # Send the whisper message to the target client
    target_client.connection.send(f"[{client.username} whispers to you: ({time.strftime('%H:%M:%S')})] {whisper_message}\n".encode())

    # Log the whisper on the server
    print(f"[{client.username} whispers to {target_username}: ({time.strftime('%H:%M:%S')})] {whisper_message}")


def switch_channel(client, channel, msg, channels):
    """
    Handles the '/switch' command to move a client to a different channel.
    """
    parts = msg.split()
    if len(parts) < 2:
        client.connection.send("Usage: /switch <channel_name>\n".encode())
        return False

    _, new_channel_name = parts

    # Check if the new channel exists
    new_channel = channels.get(new_channel_name)
    if not new_channel:
        client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] {new_channel_name} does not exist.\n".encode())
        return False

    # Check for username conflict in the new channel
    if any(c.username == client.username for c in new_channel.clients):
        client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] {new_channel_name} already has a user with username {client.username}.\n".encode())
        return False

    # Handle clients in the queue
    if client.in_queue:
        # Remove the client from the current channel's queue
        channel.queue.remove(client)

        # Update the queue for other clients
        for other_client in channel.queue:
            other_client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] Queue updated. Your position is now {channel.queue.index(other_client) + 1}.\n".encode())

        # Notify the server
        print(f"[Server message ({time.strftime('%H:%M:%S')})] {client.username} has left the channel.")

    # Handle clients in the channel
    else:
        # Remove the client from the current channel
        channel.clients.remove(client)

        # Notify other clients in the current channel
        for other_client in channel.clients:
            other_client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] {client.username} has left the channel.\n".encode())

        # Notify the server
        print(f"[Server message ({time.strftime('%H:%M:%S')})] {client.username} has left the channel.")

    # Move the client to the new channel
    new_channel.queue.put(client)  # Use append for a list
    client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] Welcome to the {new_channel_name} channel, {client.username}.\n".encode())

    # Reset the client's channel
    client.channel = new_channel

    # Correct order and use of parameters in position_client call
    position_client(new_channel, client.connection, client.username, client)

    # Notify other clients in the new channel
    for other_client in new_channel.clients:
        other_client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] {client.username} has joined the channel.\n".encode())

    # Notify the server
    print(f"[Server message ({time.strftime('%H:%M:%S')})] {client.username} has joined the {new_channel_name} channel.")

    return True

def broadcast_in_channel(client, channel, msg) -> None:
    """
    Broadcast a message to all clients in the channel, including the sender.
    """
    if client.in_queue:
        client.connection.send("You cannot send messages while in the waiting queue.\n".encode())
        return
    if client.muted:
        client.connection.send("You are currently muted and cannot send messages.\n".encode())
        return

    # Broadcast message to all clients including the sender
    formatted_message = f"[{client.username} ({time.strftime('%H:%M:%S')})] {msg}\n"
    for other_client in channel.clients:
        try:
            other_client.connection.send(formatted_message.encode())
        except Exception as e:
            print(f"Failed to send message to {other_client.username}: {e}")

def client_handler(client, channel, channels) -> None:
    while True:
        if client.kicked:
            break
        try:
            msg = client.connection.recv(1024).decode().strip()
            if not msg:
                continue  # Skip empty messages

            # Check message for client commands
            if msg.startswith("/quit"):
                quit_client(client, channel)
                break
            elif msg.startswith("/send"):
                send_client(client, channel, msg)
            elif msg.startswith("/list"):
                list_clients(client, channels)
            elif msg.startswith("/whisper"):
                whisper_client(client, channel, msg)
            elif msg.startswith("/switch"):
                if switch_channel(client, channel, msg, channels):
                    break
            else:
                # Broadcast message
                broadcast_in_channel(client, channel, msg)
                # Print message in required format on server side
                print(f"[{client.username} ({time.strftime('%H:%M:%S')})] {msg}")

            # Reset remaining time if not muted
            if not client.muted:
                client.remaining_time = 100

        except EOFError:
            continue
        except OSError:
            break
        except Exception as e:
            print(f"Error in client handler: {e}")
            break


def check_duplicate_username(username, channel, conn) -> bool:
    """
    Check if a username is already in a channel or its queue.
    Status: TODO
    """
    for client in channel.clients:
        if client.username == username:
            conn.send("Username is already in use.\n".encode())
            conn.close()
            return False
    for queued_client in list(channel.queue.queue):
        if queued_client.username == username:
            conn.send("Username is already in use in the queue.\n".encode())
            conn.close()
            return False
    return True


def position_client(channel, conn, username, new_client) -> None:
    """
    Place a client in a channel or queue based on the channel's capacity.
    Status: TODO
    """
    # Write your code here...
    if len(channel.clients) < channel.capacity:
        channel.clients.append(new_client)
        new_client.in_queue = False
        new_client.remaining_time = 100
    else:
        channel.queue.put(new_client)
        new_client.in_queue = True
        queue_position = channel.queue.qsize()
        conn.send(f"You are in the waiting queue. There are {queue_position - 1} user(s) ahead of you.\n".encode())


def channel_handler(channel, channels) -> None:
    """
    Starts a chat server, manages channels, respective queues, and incoming clients.
    Initiates different threads for channel queue processing and client handling.
    """
    # Initialize server socket, bind, and listen
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("localhost", channel.port))
    server_socket.listen(channel.capacity)

    # Launch a thread to process the client queue
    queue_thread = threading.Thread(target=process_queue, args=(channel,))
    queue_thread.start()

    while True:
        try:
            # Accept a client connection
            conn, addr = server_socket.accept()
            username = conn.recv(1024).decode()

            # Check for duplicate username in channel and its queue
            if not check_duplicate_username(username, channel, conn):
                continue  # Skip to next connection attempt if username is duplicate

            # Send a welcome message only to the new user
            welcome_msg = f"[Server message ({time.strftime('%H:%M:%S')})] Welcome to the {channel.name} channel, {username}"
            conn.send(welcome_msg.encode())
            time.sleep(0.1)  # Short pause for message delivery

            # Create and position the new client object
            new_client = Client(username, conn, addr)
            position_client(channel, conn, username, new_client)

            # Log the join on the server
            print(f"[Server message ({time.strftime('%H:%M:%S')})] {username} has joined the {channel.name} channel.")

            # Notify all clients of the new user's arrival
            join_msg = f"[Server message ({time.strftime('%H:%M:%S')})] {username} has joined the channel."
            for client in channel.clients:
                client.connection.send(join_msg.encode())

            # Start a thread for the new client
            client_thread = threading.Thread(target=client_handler, args=(new_client, channel, channels))
            client_thread.start()

        except EOFError:
            continue  # Handle disconnection gracefully
        except OSError as e:
            print(f"OSError in channel handler: {e}")
        except Exception as e:
            print(f"Unexpected error in channel handler: {e}")


def remove_item(q, item_to_remove) -> queue.Queue:
    """
    Remove item from queue
    Status: Given
    Args:
        q (queue.Queue): The queue to remove the item from.
        item_to_remove (Client): The item to remove from the queue.
    Returns:
        queue.Queue: The queue with the item removed.
    """
    new_q = queue.Queue()
    while not q.empty():
        current_item = q.get()
        if current_item != item_to_remove:
            new_q.put(current_item)

    return new_q

def process_queue(channel) -> None:
    """
    Processes the queue of clients for a channel in an infinite loop. If the channel is not full, 
    it dequeues a client, adds them to the channel, and updates their status. It then sends updates 
    to all clients in the channel and queue. The function handles EOFError exceptions and sleeps for 
    1 second between iterations.
    Status: TODO
    Args:
        channel (Channel): The channel whose queue to process.
    Returns:
        None
    """
    # Write your code here...
    while True:
        try:
            if not channel.queue.empty() and len(channel.clients) < channel.capacity:
                # Dequeue a client from the queue and add them to the channel

                # Send join message to all clients in the channel

                # Update the queue messages for remaining clients in the queue
                
                # Reset the remaining time to 100 before AFK
                time.sleep(1)
        except EOFError:
            continue

def kick_user(command, channels):
    """
    Kick a user from a specified channel.
    """
    try:
        _, channel_name, username = command.split()
    except ValueError:
        print("Invalid command format. Usage: /kick <channel_name> <username>")
        return

    # Check if the channel exists
    if channel_name not in channels:
        print(f"[Server message ({time.strftime('%H:%M:%S')})] {channel_name} does not exist.")
        return

    channel = channels[channel_name]
    # Find the client to be kicked
    client_to_kick = None
    for client in channel.clients:
        if client.username == username:
            client_to_kick = client
            break
    
    if client_to_kick:
        # Broadcast kick message to other clients
        kick_msg = f"[Server message ({time.strftime('%H:%M:%S')})] {username} has left the channel.\n"
        for other_client in channel.clients:
            if other_client != client_to_kick:
                other_client.connection.send(kick_msg.encode())

        # Log the kick on the server with "Kicked" prefix
        print(f"[Server message ({time.strftime('%H:%M:%S')})] Kicked {username}.")
        # Remove the client and close their connection
        channel.clients.remove(client_to_kick)
        client_to_kick.connection.close()
    else:
        print(f"[Server message ({time.strftime('%H:%M:%S')})] {username} is not in {channel_name}.")


def empty(command, channels) -> None:
    """
    Args:
        command (str): The command to empty a channel.
        channels (dict): A dictionary of all channels.
    """
    parts = command.split()
    if len(parts) != 2:
        print("[Server message] Usage: /empty <channel_name>")
        return

    _, channel_name = parts

    # Check if the channel exists
    if channel_name not in channels:
        print(f"[Server message ({time.strftime('%H:%M:%S')})] {channel_name} does not exist.")
        return

    channel = channels[channel_name]

    # Close connections for all clients in the channel
    for client in channel.clients:
        try:
            client.connection.send("Your connection is being terminated by the server.\n".encode())
            client.connection.close()
        except Exception as e:
            print(f"Failed to close connection for {client.username}: {e}")

    # Clear the channel's client list
    channel.clients.clear()

    # Notify the server
    print(f"[Server message ({time.strftime('%H:%M:%S')})] {channel_name} has been emptied.")


def mute_user(command, channels):
    """
    Mute a user in a specified channel for a given duration.
    """
    parts = command.split()
    if len(parts) < 4:
        print("[Server message ({})] Usage: /mute <channel_name> <username> <time>".format(time.strftime('%H:%M:%S')))
        return

    _, channel_name, username, mute_duration = parts
    
    # Validate mute duration
    try:
        mute_duration = int(mute_duration)
        if mute_duration <= 0:
            raise ValueError("Mute time must be a positive integer.")
    except ValueError:
        print("[Server message ({})] Invalid mute time.".format(time.strftime('%H:%M:%S')))
        return

    # Validate channel existence
    if channel_name not in channels:
        print("[Server message ({})] {} does not exist.".format(time.strftime('%H:%M:%S'), channel_name))
        return
    
    channel = channels[channel_name]

    # Find the user in the channel
    user_to_mute = next((c for c in channel.clients if c.username == username), None)
    if not user_to_mute:
        print("[Server message ({})] {} is not in {}.".format(time.strftime('%H:%M:%S'), username, channel_name))
        return

    # Mute the user
    user_to_mute.muted = True
    current_time = time.strftime('%H:%M:%S')
    print("[Server message ({})] Muted {} for {} seconds.".format(current_time, username, mute_duration))

    # Notify the muted user
    user_to_mute.connection.send("[Server message ({})] You have been muted for {} seconds.\n".format(current_time, mute_duration).encode())

    # Notify other clients in the channel
    for client in channel.clients:
        if client != user_to_mute:
            client.connection.send("[Server message ({})] {} has been muted for {} seconds.\n".format(current_time, username, mute_duration).encode())

    # Set a timer to unmute the user after the specified duration
    def unmute_user():
        user_to_mute.muted = False
        user_to_mute.connection.send("[Server message ({})] You are now unmuted.\n".format(time.strftime('%H:%M:%S')).encode())
        print("[Server message ({})] {} is now unmuted.".format(time.strftime('%H:%M:%S'), username))

    # Start the timer
    threading.Timer(mute_duration, unmute_user).start()


def shutdown(channels) -> None:
    """
    Implement /shutdown function
    Status: TODO
    Args:
        channels (dict): A dictionary of all channels.
    """
    # Write your code here...
    # close connections of all clients in all channels and exit the server

    # end of code insertion, keep the os._exit(0) as it is
    os._exit(0)

def server_commands(channels) -> None:
    """
    Implement commands to kick a user, empty a channel, mute a user, and shutdown the server.
    Each command has its own validation and error handling. 
    Status: Given
    Args:
        channels (dict): A dictionary of all channels.
    Returns:
        None
    """
    while True:
        try:
            command = input()
            if command.startswith('/kick'):
                kick_user(command, channels)
            elif command.startswith("/empty"):
                empty(command, channels)
            elif command.startswith("/mute"):
                mute_user(command, channels)
            elif command == "/shutdown":
                shutdown(channels)
            else:
                continue
        except EOFError:
            continue
        except Exception as e:
            print(f"{e}")
            sys.exit(1)

def check_inactive_clients(channels) -> None:
    """
    Continuously manages clients in all channels. Checks if a client is muted, in queue, or has run out of time. 
    If a client's time is up, they are removed from the channel and their connection is closed. 
    A server message is sent to all clients in the channel. The function also handles EOFError exceptions.
    Status: TODO
    Args:
        channels (dict): A dictionary of all channels.
    """
    # Write your code here...
    # parse through all the clients in all the channels

    # if client is muted or in queue, do nothing

    # remove client from the channel and close connection, print AFK message

    # if client is not muted, decrement remaining time
    pass

def handle_mute_durations(channels) -> None:
    """
    Continuously manages the mute status of clients in all channels. If a client's mute duration has expired, 
    their mute status is lifted. If a client is still muted, their mute duration is decremented. 
    The function sleeps for 0.99 seconds between iterations and handles EOFError exceptions.
    Status: Given
    Args:
        channels (dict): A dictionary of all channels.
    """
    while True:
        try:
            for channel_name in channels:
                channel = channels[channel_name]
                for client in channel.clients:
                    if client.mute_duration <= 0:
                        client.muted = False
                        client.mute_duration = 0
                    if client.muted and client.mute_duration > 0:
                        client.mute_duration -= 1
            time.sleep(0.99)
        except EOFError:
            continue

def main():
    try:
        if len(sys.argv) != 2:
            print("Usage: python3 chatserver.py configfile")
            sys.exit(1)

        config_file = sys.argv[1]

        # parsing and creating channels
        parsed_lines = parse_config(config_file)
        channels = get_channels_dictionary(parsed_lines)

        # creating individual threads to handle channels connections
        for _, channel in channels.items():
            thread = threading.Thread(target=channel_handler, args=(channel, channels))
            thread.start()

        server_commands_thread = threading.Thread(target=server_commands, args=(channels,))
        server_commands_thread.start()

        inactive_clients_thread = threading.Thread(target=check_inactive_clients, args=(channels,))
        inactive_clients_thread.start()

        mute_duration_thread = threading.Thread(target=handle_mute_durations, args=(channels,))
        mute_duration_thread.start()
    except KeyboardInterrupt:
        print("Crlt + C Pressed. Exiting...")
        os._exit(0)


if __name__ == "__main__":
    main()
