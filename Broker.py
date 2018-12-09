from socket import *
from threading import *
import string
import sys
#arguments: porta;0-broker/1-cliente;0-pub/1-sub/2-req;assunto(menos para o tipo 2);noticia apenas tipo 0 necessita.

def envia_nome(name, port):
	NameServer = 'localhost'
	NameServerPort = 12004
	clientSocket = socket(AF_INET, SOCK_STREAM)
	clientSocket.connect((NameServer,NameServerPort))
	clientSocket.send(bytes('c2c ' + name + ' ' + str(port)))
	neighbours_message = clientSocket.recv(2048)
	neighbours = neighbours_message.split(' ')
	clientSocket.close()
def aceitar_conexoes():
	while 1:
		client, addr = serverSocket.accept()
		print(str(addr) + ' conectado')
		message = client.recv(1024)
		arguments = message.split(' ')
		print(message)
		print(arguments)
		address = str(serverSocket.getsockname()[0]) + ' ' + arguments[0]
		if arguments[2] == '0': 
			Thread(target=publish, args=(arguments[3], arguments[4],address,int(arguments[1]),client)).start()
		elif arguments[2] == '1':
			Thread(target=subscribe, args=(arguments[3],address,int(arguments[1]),client)).start()
		else:
			Thread(target=request_insterest, args=(address,client)).start()
		client.close()
def publish(e1,e2,x,Type,clientSocket):
	if e1 in subscriptions:
		matchlist = match(e1, subscriptions)
		notify(e1,e2, matchlist)
		#print(matchlist)
	else:
		subscribe(e1,x,Type, clientSocket)
	if e1 in routing:
		fwdlist = match(e1,routing)
		for p in fwdlist:
			if p != x:
				ip, port = p.split(' ')
				int_port = int(port)
				clientSocket = socket(AF_INET, SOCK_STREAM)
				clientSocket.connect((ip,int_port))
				clientSocket.send(bytes(str(port) + ' 0' +' 0 ' + e1 + ' ' + e2))
				clientSocket.close()
	else:
		nameSock = str(clientSocket.getsockname()[0]) + ' ' + str(clientSocket.getsockname()[1])
		subscribe(e1,nameSock,0,clientSocket)
def subscribe(s,x,Type,clientSocket):
	if Type == 1: #tipo 1 eh cliente
		subscriptions.setdefault(s, []).append(x)
		#print('SUB')
		#print(subscriptions)
	if Type == 0: #tipo 0 eh broker
		routing.setdefault(s, []).append(x)
		#print('ROUT')
		#print(routing)
	for p in neighbours:
		if p != x:
			ip, port = p.split(' ')
			int_port = int(port)
			clientSocket = socket(AF_INET, SOCK_STREAM)
			clientSocket.connect((ip,int_port))
			clientSocket.send(bytes(str(port) + ' 0' +' 1 ' + e1))
			clientSocket.close()

def request_insterest(addr,client):
	interest_list = []
	for key,value in subscriptions.items():
		if addr in value:
			interest_list.append(key)
	
	message = ' '.join(interest_list)
	ip, port = addr.split(' ')
	int_port = int(port)
	clientSocket = socket(AF_INET, SOCK_STREAM)
	clientSocket.connect((ip,int_port))
	clientSocket.send(bytes(message))
	clientSocket.close()
	
def notify(e1,e2,matchlist):
	message = 'Assunto: ' + e1 + ' Noticia: ' + e2
	for p in matchlist:
		ip, port = p.split(' ')
		int_port = int(port)
		clientSocket = socket(AF_INET, SOCK_STREAM)
		clientSocket.connect((ip,int_port))
		clientSocket.send(bytes(message))
		clientSocket.close()
	
def match(e,lst):
	return lst[e]
	

routing = {}
subscriptions = {}
neighbours = {}

if __name__ == "__main__":
	
	name = raw_input('Insira o nome do broker\n')
	serverPort = int(raw_input('Insira a porta do broker\n'))
	envia_nome(name, serverPort)
	
	
	serverSocket = socket(AF_INET, SOCK_STREAM)
	serverSocket.bind(('', serverPort))
	serverSocket.listen(1000)
	print ('Broker Online')
	th = Thread(target=aceitar_conexoes)
	th.start()
	th.join()
	serverSocket.close()



