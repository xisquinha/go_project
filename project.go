package main

import (
	"time"
)

type Person struct {
	window_flag bool
	dog_is_out  bool
}

type Yard struct {
	yard_flag bool
	have_dog  bool
}

func handle_out_of_dog_food(yard_ch chan<- bool, person *Person) {
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
}

func main() {

	yard := Yard{true, false}

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
	}

}
