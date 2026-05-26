package main

import "testing"

func FuzzYardManager(f *testing.F) {
	// Sementes para o Fuzzing começar a gerar testes
	f.Add(int(EnterYard), int(Alice))
	f.Add(int(LeaveYard), int(Bob))

	f.Fuzz(func(t *testing.T, reqType int, clientId int) {
		// Criamos o canal que o teu código usa para gerir o quintal
		yardChan := make(chan YardRequest, 1)
		replyChan := make(chan bool, 1)

		if reqType < 0 || reqType > 3 || clientId < 0 || clientId > 2 {
			return
		}

		// Criamos o pedido com o canal de resposta embutido
		req := YardRequest{
			Type:      RequestType(reqType),
			Sender:    ClientID(clientId),
			ReplyChan: replyChan,
		}

		// Enviamos o pedido para o yardChan (simulando a Alice/Bob/Estafeta)
		yardChan <- req

		// Lemos o pedido do canal (simulando o loop do teu yardManager)
		receivedReq := <-yardChan

		// Estado simulado do quintal para testar os teus handlers
		yardFlagUp := true
		aliceDogInYard := false
		bobDogInYard := false
		pendingDeliveries := 0

		// Chamada dos teus handlers passando o pedido que veio do canal
		switch receivedReq.Type {
		case EnterYard:
			handleEnterYard(receivedReq, yardFlagUp, &aliceDogInYard, &bobDogInYard)
		case LeaveYard:
			handleLeaveYard(receivedReq, &yardFlagUp, &aliceDogInYard, &bobDogInYard, pendingDeliveries)
		case DeliveryCheck:
			handleDeliveryCheck(receivedReq, yardFlagUp, &pendingDeliveries)
		case RequestDelivery:
			handleRequestDelivery(receivedReq, &yardFlagUp, aliceDogInYard, bobDogInYard, &pendingDeliveries)
		}
	})
}
