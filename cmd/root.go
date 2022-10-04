/*
Copyright © 2022 Ron Lynn <dad@lynntribe.net>

*/
package cmd

import (
	"bufio"
	"encoding/json"
	// "errors"
	"fmt"
	"io"
	"log"
	"net"
	// "net/http"
	"net/url"
	"os"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/roncewind/szrecord"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
	exchange string = "senzing"
	inputQueue string = "senzing_input"
	inputURL string
	outputURL string
	waitGroup sync.WaitGroup
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "move",
	Short: "Move records from one location to another.",
	Long: `TODO: A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,

	// The core of this command:
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("start Run")
		fmt.Println("viper key list:")
		for _, key := range viper.AllKeys() {
			fmt.Println("  - ", key, " = ", viper.Get(key))
		}

		//TODO: test for required parameters otherwise show help.
		if( viper.IsSet("inputURL") &&
			viper.IsSet("outputURL") &&
		    viper.IsSet("exchange") &&
			viper.IsSet("inputQueue")) {

			waitGroup.Add(2)
			recordchan := make(chan string, 10)
			go read(viper.GetString("inputURL"), recordchan)
			go write(viper.GetString("outputURL"), viper.GetString("exchange"), viper.GetString("inputQueue"), recordchan)
			waitGroup.Wait()
		} else {
			cmd.Help()
		}
	},
}

// ----------------------------------------------------------------------------
func read(urlString string, recordchan chan string) {
	fmt.Println("Enter read")
	defer waitGroup.Done()
	fmt.Println("Read URL:")
	fmt.Println("URL string: ",urlString)
	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	printURL(u)
	if u.Scheme == "file" {
		if strings.HasSuffix(u.Path, "jsonl") {
			fmt.Println("Is a jsonl file.")
			readJSONL(u.Path, recordchan)
		} else {
			valid := validate(u.Path)
			fmt.Println("Is valid JSON?", valid)
			//write to channel
			close(recordchan)
		}
	} else {
		msg := fmt.Sprintf("We don't handle %s input URLs.", u.Scheme)
		panic(msg)
	}
}

// ----------------------------------------------------------------------------
func write(urlString string, exchange string, queue string, recordchan chan string) {
	fmt.Println("Enter write")
	defer waitGroup.Done()
	fmt.Println("Write URL:")
	fmt.Println("URL string: ",urlString)
	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	printURL(u)

	conn, err := amqp.Dial(urlString)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		exchange,   // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		queue, // name
		true,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name, // queue name
		q.Name,     // routing key
		exchange, // exchange
		false,
		nil,
	)

	i := 0
	for {
		i++
		// Wait for record to be assigned.
		line, result := <- recordchan

		if result == false {
			// This means the channel is empty and closed.
			fmt.Println("recordchan closed")
			return
		}

		err = ch.Publish(
			exchange,     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing {
				DeliveryMode: amqp.Persistent,
				ContentType: "text/plain",
				Body:        []byte(line),
			})
		if err != nil {
			fmt.Println("Failed to publish record ", i)
			fmt.Println("ERROR: ", err)
		}
	}
}

// ----------------------------------------------------------------------------
func readJSONL(jsonFile string, recordchan chan string) {
	file, err := os.Open(jsonFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	i := 0
	for scanner.Scan() {
		i++
		str := strings.TrimSpace(scanner.Text())
		// ignore blank lines
		if len(str) > 0 {
			valid, err := szrecord.Validate(str)
			if valid {
				recordchan <- str
			} else {
				fmt.Println("Line", i, err)
			}
		}
	}
	close(recordchan)
}

// ----------------------------------------------------------------------------
func validate(jsonFile string) bool {

	var file *os.File = os.Stdin

	if jsonFile != "" {
		var err error
		file, err = os.Open(jsonFile)
		if err != nil {
			log.Fatal(err)
		}
	}
	info, err := file.Stat()
	if err != nil {
		panic(err)
	}
	if info.Size() <= 0 {
		log.Fatal("No file found to validate.")
	}
	printFileInfo(info)

	bytes := GetBytes(file)
	if err := file.Close(); err != nil {
		log.Fatal(err)
	}

	valid := json.Valid(bytes)
	return valid
}

// ----------------------------------------------------------------------------
func GetBytes(file *os.File) []byte {

	reader := bufio.NewReader(file)
	var output []byte

	for {
		input, err := reader.ReadByte()
		if err != nil && err == io.EOF {
			break
		}
		output = append(output, input)
	}
	return output
}

// ----------------------------------------------------------------------------
func printFileInfo(info os.FileInfo) {
	fmt.Println("name: ", info.Name())
	fmt.Println("size: ", info.Size())
	fmt.Println("mode: ", info.Mode())
	fmt.Println("mod time: ", info.ModTime())
	fmt.Println("is dir: ", info.IsDir())
	if info.Mode() & os.ModeDevice == os.ModeDevice {
		fmt.Println("detected device: ", os.ModeDevice)
	}
	if info.Mode() & os.ModeCharDevice == os.ModeCharDevice {
		fmt.Println("detected char device: ", os.ModeCharDevice)
	}
	if info.Mode() & os.ModeNamedPipe == os.ModeNamedPipe {
		fmt.Println("detected named pipe: ", os.ModeNamedPipe)
	}
	fmt.Println("\n")
}

// ----------------------------------------------------------------------------
func printURL(u *url.URL) {

	fmt.Println("\tScheme: ", u.Scheme)
	fmt.Println("\tUser full: ", u.User)
	fmt.Println("\tUser name: ", u.User.Username())
	p, _ := u.User.Password()
	fmt.Println("\tPassword: ", p)

	fmt.Println("\tHost full: ", u.Host)
	host, port, _ := net.SplitHostPort(u.Host)
	fmt.Println("\tHost: ", host)
	fmt.Println("\tPort: ", port)

	fmt.Println("\tPath: ", u.Path)
	fmt.Println("\tFragment: ", u.Fragment)

	fmt.Println("\tQuery string: ", u.RawQuery)
	m, _ := url.ParseQuery(u.RawQuery)
	fmt.Println("\tParsed query string: ", m)
	// fmt.Println(m["k"][0])
}

// ----------------------------------------------------------------------------
func failOnError(err error, msg string) {
	if err != nil {
		s := fmt.Sprintf("%s: %s", msg, err)
		panic(s)
	}
}

// ----------------------------------------------------------------------------
// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the RootCmd.
func Execute() {
	err := RootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

// ----------------------------------------------------------------------------
func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.senzing-tools/config.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	// RootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	RootCmd.Flags().StringVarP(&inputURL, "inputURL", "i", "", "input location")
	viper.BindPFlag("inputURL", RootCmd.Flags().Lookup("inputURL"))
	RootCmd.Flags().StringVarP(&outputURL, "outputURL", "o", "", "output location")
	viper.BindPFlag("outputURL", RootCmd.Flags().Lookup("outputURL"))
	RootCmd.Flags().StringVarP(&exchange, "exchange", "", "", "Message queue exchange name")
	viper.BindPFlag("exchange", RootCmd.Flags().Lookup("exchange"))
	RootCmd.Flags().StringVarP(&inputQueue, "inputQueue", "", "", "Senzing input queue name")
	viper.BindPFlag("inputQueue", RootCmd.Flags().Lookup("inputQueue"))
}

// ----------------------------------------------------------------------------
// initConfig reads in config file and ENV variables if set.
// Config precedence:
// - cmdline args
// - env vars
// - config file
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in <home directory>/.senzing with name "config" (without extension).
		viper.AddConfigPath(home+"/.senzing-tools")
		viper.AddConfigPath(home)
		viper.AddConfigPath("/etc/senzing-tools")
		viper.SetConfigType("yaml")
		viper.SetConfigName("config")
	}

	viper.AutomaticEnv() // read in environment variables that match
	// all env vars should be prefixed with "SENZING_TOOLS_"
	viper.SetEnvPrefix("senzing_tools")
	viper.BindEnv("inputURL")
	viper.BindEnv("outputURL")
	viper.BindEnv("exchange")
	viper.SetDefault("exchange", "senzing")
	viper.BindEnv("inputQueue")
	viper.SetDefault("inputQueue", "senzing-input")

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}
}

