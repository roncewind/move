package mover

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/roncewind/move/io/rabbitmq"
	"github.com/roncewind/move/io/rabbitmq/managedproducer"
	"github.com/roncewind/szrecord"
)

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

type MoverImpl struct {
	FileType  string
	InputURL  string
	LogLevel  string
	OutputURL string
}

var (
	waitGroup sync.WaitGroup
)

// ----------------------------------------------------------------------------

// move records from one place to another.  validates each record as they are
// read and only moves valid records.  typically used to move records from
// a file to a queue for processing.
func (m *MoverImpl) Move() {
	waitGroup.Add(2)
	recordchan := make(chan rabbitmq.Record, 10)
	go m.read(recordchan)
	go m.write(recordchan)
	waitGroup.Wait()
}

// ----------------------------------------------------------------------------
func (m *MoverImpl) read(recordchan chan rabbitmq.Record) {

	defer waitGroup.Done()

	//This assumes the URL includes a schema and path so, minimally:
	//  "s://p" where the schema is 's' and 'p' is the complete path
	if len(m.InputURL) < 5 {
		fmt.Printf("ERROR: check the inputURL parameter: %s\n", m.InputURL)
		return
	}

	fmt.Println("inputURL: ", m.InputURL)
	u, err := url.Parse(m.InputURL)
	if err != nil {
		panic(err)
	}
	//printURL(u)
	if u.Scheme == "file" {
		if strings.HasSuffix(u.Path, "jsonl") || strings.ToUpper(m.FileType) == "JSONL" {
			fmt.Println("Reading as a JSONL file.")
			readJSONLFile(u.Path, recordchan)
		} else {
			valid := validate(u.Path)
			fmt.Println("Is valid JSON?", valid)
			//TODO: process JSON file?
			close(recordchan)
		}
	} else if u.Scheme == "http" || u.Scheme == "https" {
		if strings.HasSuffix(u.Path, "jsonl") || strings.ToUpper(m.FileType) == "JSONL" {
			fmt.Println("Reading as a JSONL resource.")
			readJSONLResource(u.Path, recordchan)
		} else {
			fmt.Println("If this is a valid JSONL file, please rename with the .jsonl extension or use the file type override (--fileType).")
		}
	} else {
		msg := fmt.Sprintf("We don't handle %s input URLs.", u.Scheme)
		panic(msg)
	}
}

// ----------------------------------------------------------------------------
func (m *MoverImpl) write(recordchan chan rabbitmq.Record) {
	fmt.Println("Enter write")
	defer waitGroup.Done()
	fmt.Println("Write URL string: ", m.OutputURL)
	u, err := url.Parse(m.OutputURL)
	if err != nil {
		panic(err)
	}
	printURL(u)

	// set number of workers to runtime.GOMAXPROCS(0)
	<-managedproducer.StartManagedProducer(m.OutputURL, runtime.GOMAXPROCS(0), recordchan)
	fmt.Println("So long and thanks for all the fish.")
}

// ----------------------------------------------------------------------------
func readJSONLResource(jsonURL string, recordchan chan rabbitmq.Record) {
	response, err := http.Get(jsonURL)
	if err != nil {
		panic(err)
	}
	defer response.Body.Close()

	scanner := bufio.NewScanner(response.Body)
	scanner.Split(bufio.ScanLines)

	fmt.Println(time.Now(), "Start resource read", jsonURL)
	i := 0
	for scanner.Scan() {
		i++
		str := strings.TrimSpace(scanner.Text())
		// ignore blank lines
		if len(str) > 0 {
			valid, err := szrecord.Validate(str)
			if valid {
				recordchan <- &record{str, jsonURL, i}
			} else {
				fmt.Println("Line", i, err)
			}
		}
		if i%10000 == 0 {
			fmt.Println(time.Now(), "Records sent to queue:", i)
		}
	}
	close(recordchan)
	fmt.Println(time.Now(), "Record channel close for resource", jsonURL)
}

// ----------------------------------------------------------------------------
func readJSONLFile(jsonFile string, recordchan chan rabbitmq.Record) {
	file, err := os.Open(jsonFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	fmt.Println(time.Now(), "Start file read", jsonFile)
	i := 0
	for scanner.Scan() {
		i++
		str := strings.TrimSpace(scanner.Text())
		// ignore blank lines
		if len(str) > 0 {
			valid, err := szrecord.Validate(str)
			if valid {
				recordchan <- &record{str, jsonFile, i}
			} else {
				fmt.Println("Line", i, err)
			}
		}
		if i%10000 == 0 {
			fmt.Println(time.Now(), "Records sent to queue:", i)
		}
	}
	close(recordchan)
	fmt.Println(time.Now(), "Record channel close for file", jsonFile)
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
	if info.Mode()&os.ModeDevice == os.ModeDevice {
		fmt.Println("detected device: ", os.ModeDevice)
	}
	if info.Mode()&os.ModeCharDevice == os.ModeCharDevice {
		fmt.Println("detected char device: ", os.ModeCharDevice)
	}
	if info.Mode()&os.ModeNamedPipe == os.ModeNamedPipe {
		fmt.Println("detected named pipe: ", os.ModeNamedPipe)
	}
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
	for key, value := range m {
		fmt.Println("Key:", key, "=>", "Value:", value[0])
	}

}

// ----------------------------------------------------------------------------
// record implementation
// ----------------------------------------------------------------------------

type record struct {
	line       string
	fileName   string
	lineNumber int
}

func (r *record) GetMessage() string {
	return r.line
}

func (r *record) GetMessageId() string {
	//TODO: meaningful or random MessageId?
	return fmt.Sprintf("%s-%d", r.fileName, r.lineNumber)
}
