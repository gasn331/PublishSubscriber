#$1$ cliente
#$2$ broker
from socket import *
from threading import *

def aceitar_conexoes():
	while 1:
		client, addr = serverSocket.accept()
		print(str(addr) + ' conectado')
		message = client.recv(1024)
		if message == '$1$':
			Thread(target=cliente, args=(client,)).start()
		else:
			Thread(target=broker, args=(client,addr)).start()
		
def cliente(client):
	
	key, value = random.choice(list(brokers_list.items()))
	client.send(bytes(value))
	client.close()
	
def broker(client,addr):
	name = client.recv(1024)
	client.close()
	brokers_list[name] = addr
			
			
serverPort = 12004
serverSocket = socket(AF_INET, SOCK_STREAM)
serverSocket.bind(('', serverPort))
brokers_list = {}


if __name__ == "__main__":
	serverSocket.listen(2)
	print ('Servidor online')
	th = Thread(target=aceitar_conexoes)
	th.start()
	th.join()
	serverSocket.close()



