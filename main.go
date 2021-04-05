/*
------------------------------------------------------------------------------
 Copyright (c) 2021 Hugo Cruz - hugo.m.cruz@gmail.com

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 THE SOFTWARE.
------------------------------------------------------------------------------
*/

package main

import (
	"fmt"
	"os"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/tkanos/gonfig"
)

func main() {

	//Configuration Data
	type Configuration struct {
		PulsarAuthenticationKey string
		PulsarURL               string
	}

	// Global variables
	var configuration Configuration

	fmt.Println("TCM Pulsar CLI v0.1")

	args := os.Args

	if len(args) != 4 && args[1] != "unsubscribe" {
		fmt.Println("Wrong arguments. Usage:")
		fmt.Println(args[0] + " unsubscribe <subscriber name> <topic>")
		os.Exit(1)

	}

	// Read the configuration file using gonfig package
	configuration = Configuration{}
	err := gonfig.GetConf("config.json", &configuration)

	if err != nil {
		fmt.Println("Error reading configuration file: " + err.Error())
		fmt.Println("Exiting now.")
		os.Exit(1)

	}

	// Pulsar Operation - Unsubscribe

	auth := pulsar.NewAuthenticationToken(configuration.PulsarAuthenticationKey)

	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: configuration.PulsarURL, Authentication: auth})

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            args[3],
		SubscriptionName: args[2],
		Type:             pulsar.Shared,
	})

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer consumer.Close()

	if err := consumer.Unsubscribe(); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("Subscriber removed successfully.")
	}

	// Close client when ending
	client.Close()

}
