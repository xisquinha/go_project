package main

import (
	"fmt"
	"time"
)

/*type Person struct {
	window_flag bool
	dog_is_out  bool
}

type Yard struct {
	yard_flag bool
	have_dog  bool
}*/

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

// depois é melhor criar metodos auxiliares tipo handle_EnterYard, handle_LeaveYard, ...
// para o metodo n ficar gigante
func yardManager(requestChan chan YardRequest) {
	//o yard sabe da sua flag e sabe se o cao de alguem está lá
	yardFlagUp := true
	aliceDogInYard := false
	bobDogInYard := false

	for req := range requestChan {
		switch req.Type {
		case EnterYard: //EnterYard é quando o uma pessoa quer que o seu cao entre no yard, coitatinho
			// Logica para autorizar ou não a entrada do cão
			if !yardFlagUp && !aliceDogInYard && !bobDogInYard {
				// So pode entrar se a comida já foi recolhida (yardFlagUp == false)
				// E nenhum cão está lá

				//Cao respetivo entra no yard
				if req.Sender == Alice {
					aliceDogInYard = true
				} else if req.Sender == Bob {
					bobDogInYard = true
				}

				req.ReplyChan <- true // responde a dizer que pode :)
			} else {
				req.ReplyChan <- false // o cao nao pode ir pro yard, pk ta ocupado ou com comida por recolher, coitadinho
			}
		case LeaveYard:
			// caozinho sai
			if req.Sender == Alice {
				aliceDogInYard = false
			} else if req.Sender == Bob {
				bobDogInYard = false
			}

			req.ReplyChan <- true // Respondemos a dizer que o yard está livre agora ou será que significa so que tipo ok ele saiu com sucesso(?)

		case DeliveryCheck: //quando vem a delivery, só pode vir se a yardFlag estiver up
			if yardFlagUp {
				yardFlagUp = false    // "delivery person mete a flag down quando vai embora"
				req.ReplyChan <- true // pode fazer a delivery
			} else {
				req.ReplyChan <- false // nao pode fazer a delivery
			}

		case RequestDelivery: // Alice/Bob pedem comida
			// Quando ficam sem comida, eles fazem arrest do cão em casa e metem a flag up
			if !aliceDogInYard && !bobDogInYard {
				yardFlagUp = true
				req.ReplyChan <- true // flag true, delivery já pode vir
			} else {
				req.ReplyChan <- false
			}

		}

	}
}

/*func handle_out_of_dog_food(yard_ch chan<- bool, person *Person) {
	*&person.dog_is_out = false
	*&person.window_flag = false

	yard_ch <- true
}

func person(person *Person, my_window chan bool, neighbour_window chan bool, yard_ch chan bool) {

	timeout_dog_want_leave := time.After(30 * time.Second)

	select {

	case <-timeout_dog_want_leave:
		neighbour_flag := <-neighbour_window
		if !neighbour_flag {
			yard_flag := <-yard_ch

			if !yard_flag {
				*&person.dog_is_out = true
				*&person.window_flag = true
			}
		}

	}
}*/

func person(person ClientID, yardChan chan YardRequest, orderChan chan Order, myWindow chan bool, neighbourWindow chan bool) {
	personName := clientIdToString(person)
	myReplyChan := make(chan bool)
	deliveryReplyChan := make(chan bool) // Canal privado para o delivery nos responder
	hasFood := true

	//flag da window começa a false
	myWindow <- false

	for {
		if !hasFood {
			fmt.Printf("[%s] Oh não, acabou a comida! Vou trancar o cão e a pedir entrega...\n", personName)

			// Tenta levantar a flag no quintal. Fica a tentar varias vezes porque o cao do outro vizinho pode estar lá no yard,
			// entao nao vamos expulsar o cao de lá so para encomendar comida e meter a flag up
			flagUp := false
			for !flagUp {
				yardChan <- YardRequest{Type: RequestDelivery, Sender: person, ReplyChan: myReplyChan}
				flagUp = <-myReplyChan
				if !flagUp { //o yard responde false quando não é ainda possivel levantar a flag para se encomendar comida
					time.Sleep(500 * time.Millisecond) // Espera o cão entrar se ele ainda estiver lá fora
				}
			}

			fmt.Printf("[%s] YardFlag está up. A encomendar comida online...\n", personName)
			orderChan <- Order{Customer: person, ReplyChan: deliveryReplyChan} // envia pedido de delivery

			// bloqueia até receber a encomenda (ou seja até o delivery person responder)
			<-deliveryReplyChan

			fmt.Printf("[%s] A encomenda chegou! Fui buscar a comida ao quintal.\n", personName)
			hasFood = true
		}

		// --- O CÃO QUER IR AO YARD ---
		fmt.Printf("[%s] O cão quer ir dar uma volta ao quintal...\n", personName)

		// "When one of them wants to release their pet, both flags must be down."
		myWindowFlag := <-myWindow // Primeiro, verificamos a nossa própria janela (tiramos o valor para ler)

		neighbourWindowFlag := true // Assumimos true por segurança até conseguir ler

		// Tentamos "espreitar" a janela do vizinho sem bloquear o programa
		select {
		case neighbourWindowFlag = <-neighbourWindow:
			// palavras sábias do nosso amigo:
			// Conseguimos ler o estado real do vizinho!
			// Mas atenção: como lemos o valor, TEMOS de o devolver imediatamente para o canal do vizinho
			// para ele não ficar sem a flag dele! (Isto é crucial em Go)
			neighbourWindow <- neighbourWindowFlag
		default:
			// Se o canal do vizinho estiver temporariamente bloqueado, assumimos true para não soltar o nosso cão indevidamente
			neighbourWindowFlag = true
		}

		// Se ambas as janelas estão DOWN, podemos tentar pedir o quintal ao yardManager
		if !myWindowFlag && !neighbourWindowFlag {

			yardChan <- YardRequest{Type: EnterYard, Sender: person, ReplyChan: myReplyChan}

			allowed := <-myReplyChan
			if allowed {

				// "When a pet is released, the flag of its house is first put up."
				myWindow <- true // Atualizamos a nossa janela para UP

				fmt.Printf("[%s] Janelas OK e Quintal Livre! O cão entrou no quintal por 5s.\n", personName)

				// Em vez de um Sleep fixo, usamos um select com o tempo limite no quintal
				select {
				case <-time.After(5 * time.Second):
					fmt.Printf("[%s] Tempo máximo esgotado! A chamar o cão para dentro.\n", personName)
				}

				// Força a saída do cão
				yardChan <- YardRequest{Type: LeaveYard, ReplyChan: myReplyChan}
				<-myReplyChan

				// "When the pet returns, the flag of its house is put down."
				<-myWindow        // Remove o "true" antigo
				myWindow <- false // Poe a janela a false, porque o cao saiu

				// Assumimos que o cao so come dentro de casa, parece me fazer sentido
				fmt.Printf("[%s] O cão está em casa a descansar e a comer...\n", personName)
				time.Sleep(10 * time.Second) // Passado este tempo a comida acaba
				hasFood = false
				continue
			}
		}
		// Se não foi permitido (janelas ocupadas ou quintal cheio),
		// devolvemos o nosso estado original à nossa janela e esperamos antes de tentar de novo
		myWindow <- myWindowFlag
		time.Sleep(1 * time.Second)
	}
}

func clientIdToString(client ClientID) string {
	if client == Alice {
		return "Alice"
	} else if client == Bob {
		return "Bob"
	} else if client == DeliveryPerson {
		return "Delivery Person"
	} else {
		return "Unknown"
	}
}

func main() {

	yardChan := make(chan YardRequest)
	orderChan := make(chan Order)

	// Canais das janelas (capacidade 1 para guardar o estado)
	aliceWindow := make(chan bool, 1)
	bobWindow := make(chan bool, 1)

	go yardManager(yardChan)
	go deliveryPerson(yardChan, orderChan) //Falta fazer este

	// Alice
	go person(Alice, yardChan, orderChan, aliceWindow, bobWindow)
	// Bob
	go person(Bob, yardChan, orderChan, bobWindow, aliceWindow)

	// Mantém o programa principal vivo, ou entao usamos WaitGroup, mas tmb as go routines nunca acabam entao tanto faz
	select {}

	/*yard := Yard{true, false}

	alice := Person{false, false}
	bob := Person{false, false}

	ch_alice_yard := make(chan bool)

	alice_window := make(chan bool)
	bob_window := make(chan bool)

	timeout := time.After(10 * time.Second)

	for {
		select {
		case <-timeout:
			go handle_out_of_dog_food(ch_alice_yard, &alice)

		}
	}*/

}
