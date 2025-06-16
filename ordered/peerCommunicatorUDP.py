from socket  import *
from constMP import * #-
import threading
import random
import time
import pickle
from requests import get
import heapq

#handShakes = [] # not used; only if we need to check whose handshake is missing

# Counter to make sure we have received handshakes from all other processes
handShakeCount = 0

PEERS = []

# Variável global para o relógio de Lamport
lamport_clock = 0
# Fila de prioridade para armazenar mensagens recebidas
# Os elementos serão tuplas: (timestamp, ID_remetente, número_mensagem)
message_queue = []
# Lock para proteger o acesso ao relógio de Lamport e à fila de mensagens
clock_and_queue_lock = threading.Lock()

count = 5

# UDP sockets to send and receive data messages:
# Create send socket
sendSocket = socket(AF_INET, SOCK_DGRAM)
#Create and bind receive socket
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# TCP socket to receive start signal from the comparison server:
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)


def get_public_ip():
  ipAddr = get('https://api.ipify.org').content.decode('utf8')
  print('My public IP address is: {}'.format(ipAddr))
  return ipAddr

# Function to register this peer with the group manager
def registerWithGroupManager():
  clientSock = socket(AF_INET, SOCK_STREAM)
  print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  ipAddr = get_public_ip()
  req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
  msg = pickle.dumps(req)
  print ('Registering with group manager: ', req)
  clientSock.send(msg)
  clientSock.close()

def getListOfPeers():
  clientSock = socket(AF_INET, SOCK_STREAM)
  print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  req = {"op":"list"}
  msg = pickle.dumps(req)
  print ('Getting list of peers from group manager: ', req)
  clientSock.send(msg)
  msg = clientSock.recv(2048)
  PEERS = pickle.loads(msg)
  print ('Got list of peers: ', PEERS)
  clientSock.close()
  return PEERS

class MsgHandler(threading.Thread):
  def __init__(self, sock):
    threading.Thread.__init__(self)
    self.sock = sock

  def run(self):
    print('Handler is ready. Waiting for the handshakes...')
    
    #global handShakes
    global handShakeCount
    global lamport_clock # Acessa o relógio de Lamport
    
    logList = []
    
    # Wait until handshakes are received from all other processes
    # (to make sure that all processes are synchronized before they start exchanging messages)
    while handShakeCount < N:
      msgPack = self.sock.recv(1024)
      msg = pickle.loads(msgPack)
      #print ('########## unpickled msgPack: ', msg)
      if msg[0] == 'READY':
        # Atualiza o relógio de Lamport ao receber um handshake
        with clock_and_queue_lock:
          lamport_clock = max(lamport_clock, msg[2]) + 1 # msg[2] será o timestamp do handshake
        # To do: send reply of handshake and wait for confirmation

        handShakeCount = handShakeCount + 1
        #handShakes[msg[1]] = 1
        print('--- Handshake received: ', msg[1])

    print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

    stopCount=0 
    
    while True:                
      msgPack = self.sock.recv(1024)   # receive data from client
      msg = pickle.loads(msgPack)
      global count
      
      if msg[0] == -1:   # count the 'stop' messages from the other processes
        stopCount = stopCount + 1
        if stopCount == N:
          break  # stop loop when all other processes have finished
      else:
        # msg[0]: ID do remetente, msg[1]: número da mensagem, msg[2]: timestamp de Lamport
        sender_id, msg_number, msg_timestamp = msg[0], msg[1], msg[2]
                
        with clock_and_queue_lock:
          # Atualiza o relógio de Lamport
          lamport_clock = max(lamport_clock, msg_timestamp) + 1
          # Adiciona a mensagem à fila de prioridade
          # Usamos o timestamp como prioridade principal e o ID do remetente como desempate
          heapq.heappush(message_queue, (msg_timestamp, sender_id, msg_number))
                
        print(f'Message {count} from process {myself} in response to Message {msg_number} from process {sender_id}')
        count += 1
      
      # Tenta entregar as mensagens ordenadamente
      self.deliver_ordered_messages(logList)
    
    # Garante que todas as mensagens na fila sejam entregues antes de finalizar
    while message_queue:
      self.deliver_ordered_messages(logList)

    # Write log file
    logFile = open('logfile'+str(myself)+'.log', 'w')
    logFile.writelines(str(logList))
    logFile.close()
    
    # Send the list of messages to the server (using a TCP socket) for comparison
    print('Sending the list of messages to the server for comparison...')
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((SERVER_ADDR, SERVER_PORT))
    msgPack = pickle.dumps(logList)
    clientSock.send(msgPack)
    clientSock.close()
    
    # Reset the handshake counter
    handShakeCount = 0

    exit(0)

  def deliver_ordered_messages(self, logList):
    global lamport_clock
    global N # N é o número total de peers

    while message_queue:
      # Pega a mensagem com o menor timestamp da fila (não remove ainda)
      next_msg_timestamp, next_msg_sender_id, next_msg_number = message_queue[0]
    
      with clock_and_queue_lock:
        popped_msg = heapq.heappop(message_queue)

      logList.append((popped_msg[1], popped_msg[2])) # Armazena (ID_remetente, número_mensagem)
      # print(f'DELIVERED: Message {popped_msg[2]} from process {popped_msg[1]} (LC: {popped_msg[0]})')

# Function to wait for start signal from comparison server:
def waitToStart():
  (conn, addr) = serverSock.accept()
  msgPack = conn.recv(1024)
  msg = pickle.loads(msgPack)
  myself = msg[0]
  nMsgs = msg[1]
  conn.send(pickle.dumps('Peer process '+str(myself)+' started.'))
  conn.close()
  return (myself,nMsgs)

# From here, code is executed when program starts:
registerWithGroupManager()
while 1:
  print('Waiting for signal to start...')
  (myself, nMsgs) = waitToStart()
  print('I am up, and my ID is: ', str(myself))

  if nMsgs == 0:
    print('Terminating.')
    exit(0)

  # Wait for other processes to be ready
  # To Do: fix bug that causes a failure when not all processes are started within this time
  # (fully started processes start sending data messages, which the others try to interpret as control messages) 
  time.sleep(5)

  # Create receiving message handler
  msgHandler = MsgHandler(recvSocket)
  msgHandler.start()
  print('Handler started')

  PEERS = getListOfPeers()
  
  # Send handshakes
  # To do: Must continue sending until it gets a reply from each process
  #        Send confirmation of reply
  for addrToSend in PEERS:
    print('Sending handshake to ', addrToSend)
    with clock_and_queue_lock:
      lamport_clock += 1 # Incrementa o relógio antes de enviar
      msg = ('READY', myself, lamport_clock) # Inclui o timestamp de Lamport
    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
    #data = recvSocket.recvfrom(128) # Handshadke confirmations have not yet been implemented

  print('Main Thread: Sent all handshakes. handShakeCount=', str(handShakeCount))

  while (handShakeCount < N):
    pass  # find a better way to wait for the handshakes

  # Send a sequence of data messages to all other processes 
  for msgNumber in range(0, nMsgs):
    # Wait some random time between successive messages
    time.sleep(random.randrange(10,100)/1000)
    with clock_and_queue_lock:
      lamport_clock += 1 # Incrementa o relógio antes de enviar
      msg = (myself, msgNumber, lamport_clock) # Inclui o timestamp de Lamport
        
    msgPack = pickle.dumps(msg)
    for addrToSend in PEERS:
      sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
      print(f'Message {msgNumber} from process {myself}')

  # Tell all processes that I have no more messages to send
  for addrToSend in PEERS:
    with clock_and_queue_lock:
      lamport_clock += 1 # Incrementa o relógio ao enviar a mensagem de parada
      msg = (-1,-1, lamport_clock) # Inclui o timestamp de Lamport

    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))