package main

import (
	"fmt"
	"os"
	"runtime/pprof"
	"time"
)

// usamos esta cena que é basicamente um enum RequestType
// que pode ser EnterYard, LeaveYard ou DeliveryCheck
type RequestType int

const (
	EnterYard RequestType = iota
	LeaveYard
	RequestDelivery
	DeliveryCheck
)

// mesma cena para quem manda resquet ao yard,
// precisamos de saber se foi a alice ou o bob, para o yarn poder registar qual dos caes está lá
type ClientID int

const (
	Alice ClientID = iota
	Bob
	DeliveryPerson // Também nos vai dar jeito para o estafeta!
)

// temos um yard request, que é o tipo de cena que enviamos ao channel do yard
// no resquest temos o type e o canal que o yard usa para responder e tambem o sender
// por exemplo se a alice for falar com o yard, vai dar um canal dela no qual ela quer que o yard lhe responda
type YardRequest struct {
	Type      RequestType
	Sender    ClientID
	ReplyChan chan bool
}

type Order struct {
	Customer  ClientID
	ReplyChan chan bool // Canal privado de quem fez a order, para o delivery ir diretamente para ele
}

// ----------------------------- AUX FUNC YARD ---------------------------------

// qquando o uma pessoa quer que o seu cao entre no yard, coitatinho
func handleEnterYard(req YardRequest, yardFlagUp bool, aliceDogInYard *bool, bobDogInYard *bool) {
	// Logica para autorizar ou não a entrada do cão
	if !yardFlagUp && !*aliceDogInYard && !*bobDogInYard {
		// So pode entrar se a comida já foi recolhida (yardFlagUp == false)
		// E nenhum cão está lá

		//Cao respetivo entra no yard
		switch req.Sender {
		case Alice:
			*aliceDogInYard = true
		case Bob:
			*bobDogInYard = true
		}

		req.ReplyChan <- true // responde a dizer que pode :)
	} else {
		req.ReplyChan <- false // o cao nao pode ir pro yard, pk ta ocupado ou com comida por recolher, coitadinho
	}
}

// Quando um cão ou o delivery man sai do yard
func handleLeaveYard(req YardRequest, yardFlagUp *bool, aliceDogInYard *bool, bobDogInYard *bool, pendingDeliveries int) {
	// caozinho sai, ão ão
	switch req.Sender {
	case Alice:
		*aliceDogInYard = false
	case Bob:
		*bobDogInYard = false
	case DeliveryPerson:
		*yardFlagUp = false // "delivery person mete a flag down quando vai embora"
	}

	if pendingDeliveries > 0 {
		*yardFlagUp = true
	}

	req.ReplyChan <- true // Respondemos a dizer que já se pode entrar no yard, nós autorizamos
}

// Quando vem a delivery, só pode vir se a yardFlag estiver up
func handleDeliveryCheck(req YardRequest, yardFlagUp bool, pendingDeliveries *int) {
	if yardFlagUp {
		*pendingDeliveries--
		req.ReplyChan <- true // pode fazer a delivery
	} else {
		req.ReplyChan <- false // nao pode fazer a delivery
	}
}

// Alice/Bob/cão pedem comida
func handleRequestDelivery(req YardRequest, yardFlagUp *bool, aliceDogInYard bool, bobDogInYard bool, pendingDeliveries *int) {
	// Quando ficam sem comida, eles fazem arrest do cão em casa e metem a flag up
	if !aliceDogInYard && !bobDogInYard {
		*pendingDeliveries++
		*yardFlagUp = true
		req.ReplyChan <- true // flag true, delivery já pode vir
	} else {
		req.ReplyChan <- false
	}
}

// ------------------------------ YARD MANAGER ---------------------------------

// função principal do yard, chama os handlers todos
func yardManager(requestChan chan YardRequest) {
	//o yard sabe da sua flag e sabe se o cao de alguem está lá
	yardFlagUp := true
	aliceDogInYard := false
	bobDogInYard := false
	pendingDeliveries := 0

	for req := range requestChan {
		switch req.Type {
		case EnterYard:
			handleEnterYard(req, yardFlagUp, &aliceDogInYard, &bobDogInYard)
		case LeaveYard:
			handleLeaveYard(req, &yardFlagUp, &aliceDogInYard, &bobDogInYard, pendingDeliveries)
		case DeliveryCheck:
			handleDeliveryCheck(req, yardFlagUp, &pendingDeliveries)
		case RequestDelivery:
			handleRequestDelivery(req, &yardFlagUp, aliceDogInYard, bobDogInYard, &pendingDeliveries)
		}
	}
}

// ---------------------------- DELIVERY MAN -----------------------------------

/**
* yardChan --> channel to communicate with the yard manager
* orderChan --> to receive food order from alice or bob
**/
func deliveryPerson(yardChan chan YardRequest, orderChan chan Order) {
	// private reply channel to talk to the yard manager
	replyChan := make(chan bool)

	// infinite loop that blocks and wait for alice or bob to ordering food
	for order := range orderChan {

		fmt.Printf("[Delivery] Recebi encomenda de %s! A tentar entrar no quintal, "+
			"se algum cão me morder, vou processar-vos...\n", clientIdToString(order.Customer))

		// delivery man keeps trying to enter the yard, some dog can still be out there
		delivered := false
		for !delivered { // while do go
			yardChan <- YardRequest{Type: DeliveryCheck, Sender: DeliveryPerson, ReplyChan: replyChan}
			allowed := <-replyChan

			if allowed {
				fmt.Printf("[Delivery] Flag está up! A entregar comida no quintal...\n")
				time.Sleep(2 * time.Second) // simulate time in yard
				fmt.Printf("[Delivery] Entrega feita! A sair do quintal.\n")

				// telling the yard manager that we are leaving
				yardChan <- YardRequest{Type: LeaveYard, Sender: DeliveryPerson, ReplyChan: replyChan}
				<-replyChan

				order.ReplyChan <- true // notify the customer directly via their private channel
				delivered = true
			} else {
				fmt.Printf("[Delivery] Flag está down, ainda está um cão no quintal. " +
					"Vou fumar um nite, não preciso disto...\n")
				time.Sleep(500 * time.Millisecond)
			}
		}
	}
}

// ----------------------------- AUX FUNC PERSON -------------------------------

func orderFood(person ClientID, personName string, yardChan chan YardRequest, orderChan chan Order, myReplyChan chan bool) {
	fmt.Printf("[%s] Oh não, acabou a comida! Vou trancar o cão e a pedir entrega...\n", personName)

	// Tenta levantar a flag no quintal. Fica a tentar varias vezes porque o cao do outro vizinho pode estar lá no yard,
	// entao nao vamos expulsar o cao de lá so para encomendar comida e meter a flag up
	for {
		yardChan <- YardRequest{Type: RequestDelivery, Sender: person, ReplyChan: myReplyChan}
		//o yard responde false quando não é ainda possivel levantar a flag para se encomendar comida
		// só saimos do loop quando recebermos um true
		if <-myReplyChan {
			break
		}
		time.Sleep(500 * time.Millisecond) // Espera o cão entrar se ele ainda estiver lá fora
	}

	fmt.Printf("[%s] YardFlag está up. A encomendar comida online...\n", personName)

	deliveryReplyChan := make(chan bool)
	orderChan <- Order{Customer: person, ReplyChan: deliveryReplyChan} // envia pedido de delivery
	// bloqueia até receber a encomenda (ou seja até o delivery person responder)
	<-deliveryReplyChan

	fmt.Printf("[%s] A encomenda chegou! Fui buscar a comida ao quintal.\n", personName)
}

func checkWindows(myWindow chan bool, neighbourWindow chan bool) (bool, bool) {
	// "When one of them wants to release their pet, both flags must be down."
	myWindowFlag := false
	select {
	case val := <-myWindow:
		myWindowFlag = val
		// don't put back — we own it, letDogOut/person will restore it
	default:
		myWindowFlag = true
	}

	// Tentamos "espreitar" a janela do vizinho sem bloquear o programa
	// palavras sábias do nosso amigo:
	// Conseguimos ler o estado real do vizinho!
	// Mas atenção: como lemos o valor, TEMOS de o devolver imediatamente para o canal do vizinho
	// para ele não ficar sem a flag dele! (Isto é crucial em Go)
	neighbourWindowFlag := false

	select {
	case val := <-neighbourWindow:
		neighbourWindowFlag = val
		neighbourWindow <- val // immediately put back
	default:
		neighbourWindowFlag = true
	}

	return !myWindowFlag && !neighbourWindowFlag, myWindowFlag
}

func letDogOut(person ClientID, personName string, yardChan chan YardRequest, myWindow chan bool, myReplyChan chan bool) bool {
	yardChan <- YardRequest{Type: EnterYard, Sender: person, ReplyChan: myReplyChan}
	allowed := <-myReplyChan

	if !allowed {
		myWindow <- false
		return false
	}

	// "When a pet is released, the flag of its house is first put up."
	myWindow <- true // Atualizamos a nossa janela para UP
	fmt.Printf("[%s] Janelas OK e Quintal Livre! O cão entrou no quintal por 5s.\n", personName)

	<-time.After(5 * time.Second)
	fmt.Printf("[%s] Tempo máximo esgotado! A chamar o cão para dentro.\n", personName)

	// Força a saída do cão
	yardChan <- YardRequest{Type: LeaveYard, Sender: person, ReplyChan: myReplyChan}
	<-myReplyChan

	// "When the pet returns, the flag of its house is put down."
	<-myWindow        // Remove o "true" antigo
	myWindow <- false // Poe a janela a false, porque o cao saiu

	// Assumimos que o cao so come dentro de casa, parece me fazer sentido
	fmt.Printf("[%s] O cão está em casa a descansar e a comer...\n", personName)
	return true
}

// -------------------------------- PERSON -------------------------------------

func person(person ClientID, yardChan chan YardRequest, orderChan chan Order, myWindow chan bool, neighbourWindow chan bool) {
	personName := clientIdToString(person)
	myReplyChan := make(chan bool)
	hasFood := false

	myWindow <- false

	for {
		if !hasFood {
			orderFood(person, personName, yardChan, orderChan, myReplyChan)
			hasFood = true
		}

		fmt.Printf("[%s] O cão quer ir dar uma volta ao quintal...\n", personName)

		canDogGoOut, myWindowFlag := checkWindows(myWindow, neighbourWindow)

		// Se ambas as janelas estão DOWN, podemos tentar pedir o quintal ao yardManager
		if canDogGoOut {
			dogWentOut := letDogOut(person, personName, yardChan, myWindow, myReplyChan)
			if dogWentOut {
				time.Sleep(10 * time.Second) // Passado este tempo a comida acaba
				hasFood = false
			} else {
				time.Sleep(1 * time.Second)
			}
		} else {
			// Se não foi permitido (janelas ocupadas ou quintal cheio)
			// devolvemos o nosso estado original à nossa janela e esperamos antes de tentar de novo
			myWindow <- myWindowFlag
			time.Sleep(1 * time.Second)
		}
	}
}

// --------------------------- MAIN E OUTRAS CENAS -----------------------------

func clientIdToString(client ClientID) string {

	switch client {
	case Alice:
		return "Alice"
	case Bob:
		return "Bob"
	case DeliveryPerson:
		return "Delivery Person"
	default:
		return "Unknown"
	}
}

func main() {

	f, err := os.Create("goroutine_profile.prof")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	yardChan := make(chan YardRequest)
	orderChan := make(chan Order) // channel for alice and bob order food

	// Canais das janelas (capacidade 1 para guardar o estado)
	aliceWindow := make(chan bool, 1)
	bobWindow := make(chan bool, 1)

	go yardManager(yardChan)
	go deliveryPerson(yardChan, orderChan)

	// Alice
	go person(Alice, yardChan, orderChan, aliceWindow, bobWindow)
	// Bob
	go person(Bob, yardChan, orderChan, bobWindow, aliceWindow)

	pprof.Lookup("goroutine").WriteTo(f, 0)

	// Mantém o programa principal vivo, ou entao usamos WaitGroup, mas tmb as go routines nunca acabam entao tanto faz
	select {}

}
