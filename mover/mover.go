package mover

import (
	"bufio"
	"compress/gzip"
	"context"
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

	"github.com/roncewind/go-util/queues"
	"github.com/roncewind/move/io/rabbitmq"
	"github.com/senzing/go-common/record"
)

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

type MoverError struct {
	error
}

type MoverImpl struct {
	FileType  string
	InputURL  string
	LogLevel  string
	OutputURL string
}

// ----------------------------------------------------------------------------

// Check at compile time that the implementation adheres to the interface.
var _ Mover = (*MoverImpl)(nil)

var (
	waitGroup sync.WaitGroup
)

// ----------------------------------------------------------------------------

// move records from one place to another.  validates each record as they are
// read and only moves valid records.  typically used to move records from
// a file to a queue for processing.
func (m *MoverImpl) Move(ctx context.Context) {
	waitGroup.Add(2)
	recordchan := make(chan queues.Record, 10)
	go m.read(ctx, recordchan)
	go m.write(ctx, recordchan)
	waitGroup.Wait()
}

// ----------------------------------------------------------------------------
// -- Write implementation: writes records in the record channel to the output
// ----------------------------------------------------------------------------

// this function implements writing to RabbitMQ
func (m *MoverImpl) write(ctx context.Context, recordchan chan queues.Record) {

	defer waitGroup.Done()

	outputURL := m.OutputURL
	outputURLLen := len(outputURL)

	if outputURLLen == 0 {
		//assume stdout
		writeStdout(recordchan)
		return
	}

	//This assumes the URL includes a schema and path so, minimally:
	//  "s://p" where the schema is 's' and 'p' is the complete path
	if len(outputURL) < 5 {
		fmt.Printf("ERROR: check the inputURL parameter: %s\n", outputURL)
		return
	}

	fmt.Println("outputURL: ", outputURL)
	u, err := url.Parse(outputURL)
	if err != nil {
		panic(err)
	}
	printURL(u)
	switch u.Scheme {
	case "amqp":
		rabbitmq.StartProducer(ctx, outputURL, runtime.GOMAXPROCS(0), recordchan)
	case "file":
		success := true
		if strings.HasSuffix(u.Path, "jsonl") || strings.ToUpper(m.FileType) == "JSONL" {
			fmt.Println("Reading as a JSONL file.")
			success = writeJSONLFile(u.Path, recordchan)
		} else if strings.HasSuffix(u.Path, "gz") || strings.ToUpper(m.FileType) == "GZ" {
			fmt.Println("Reading as a GZ file.")
			success = writeGZFile(u.Path, recordchan)
		} else {
			valid := validate(u.Path)
			fmt.Println("Is valid JSON?", valid)
			//TODO: process JSON file?
			fmt.Println("Only able to process JSON-Lines files at this time.")
			success = false
		}
		if !success {
			panic("Unable to continue.")
		}
	case "sqs":
		fmt.Println("Move to SQS not implemented.")
	default:
		fmt.Println("Unknown URL Scheme.  Unable to write to:", outputURL)
	}
	fmt.Println("So long and thanks for all the fish.")
}

// ----------------------------------------------------------------------------

func writeStdout(recordchan chan queues.Record) bool {
	_, err := os.Stdout.Stat()
	if err != nil {
		fmt.Println("Fatal error opening stdout.", err)
		return false
	}
	// printFileInfo(info)

	writer := bufio.NewWriter(os.Stdout)
	for record := range recordchan {
		writer.WriteString(record.GetMessage())
		writer.WriteString("\n")
	}
	err = writer.Flush()
	if err != nil {
		fmt.Println("Error flushing stdout", err)
		return false
	}
	return true
}

// ----------------------------------------------------------------------------

func writeJSONLFile(fileName string, recordchan chan queues.Record) bool {
	_, err := os.Stat(fileName)
	if err == nil { //file exists
		fmt.Println("Error output file", fileName, "exists.")
		return false
	}
	f, err := os.Create(fileName)
	defer f.Close()
	if err != nil {
		fmt.Println("Fatal error opening", fileName, err)
		return false
	}
	info, err := f.Stat()
	if err != nil {
		fmt.Println("Fatal error opening", fileName, err)
		return false
	}
	printFileInfo(info)

	writer := bufio.NewWriter(f)
	for record := range recordchan {
		writer.WriteString(record.GetMessage())
		writer.WriteString("\n")
	}
	err = writer.Flush()
	if err != nil {
		fmt.Println("Error flushing", fileName, err)
		return false
	}
	return true
}

// ----------------------------------------------------------------------------

func writeGZFile(fileName string, recordchan chan queues.Record) bool {
	_, err := os.Stat(fileName)
	if err == nil { //file exists
		fmt.Println("Error output file", fileName, "exists.")
		return false
	}
	f, err := os.Create(fileName)
	defer f.Close()
	if err != nil {
		fmt.Println("Fatal error opening", fileName, err)
		return false
	}
	info, err := f.Stat()
	if err != nil {
		fmt.Println("Fatal error opening", fileName, err)
		return false
	}
	printFileInfo(info)
	gzfile := gzip.NewWriter(f)
	defer gzfile.Close()
	writer := bufio.NewWriter(gzfile)
	for record := range recordchan {
		writer.WriteString(record.GetMessage())
		writer.WriteString("\n")
	}
	err = writer.Flush()
	if err != nil {
		fmt.Println("Error flushing", fileName, err)
		return false
	}
	return true
}

// ----------------------------------------------------------------------------
// -- Read implementation: reads records from the input to the record channel
// ----------------------------------------------------------------------------

// this function attempts to determine the source of records.
// it then parses the source and puts the records into the record channel.
func (m *MoverImpl) read(ctx context.Context, recordchan chan queues.Record) {

	defer waitGroup.Done()

	inputURL := m.InputURL
	inputURLLen := len(inputURL)

	if inputURLLen == 0 {
		//assume stdin
		readStdin(recordchan)
		return
	}

	//This assumes the URL includes a schema and path so, minimally:
	//  "s://p" where the schema is 's' and 'p' is the complete path
	if len(inputURL) < 5 {
		fmt.Printf("ERROR: check the inputURL parameter: %s\n", inputURL)
		return
	}

	fmt.Println("inputURL: ", inputURL)
	u, err := url.Parse(inputURL)
	if err != nil {
		panic(err)
	}
	//printURL(u)
	if u.Scheme == "file" {
		if strings.HasSuffix(u.Path, "jsonl") || strings.ToUpper(m.FileType) == "JSONL" {
			fmt.Println("Reading as a JSONL file.")
			readJSONLFile(u.Path, recordchan)
		} else if strings.HasSuffix(u.Path, "gz") || strings.ToUpper(m.FileType) == "GZ" {
			fmt.Println("Reading as a GZ file.")
			readGZFile(u.Path, recordchan)
		} else {
			valid := validate(u.Path)
			fmt.Println("Is valid JSON?", valid)
			//TODO: process JSON file?
			close(recordchan)
		}
	} else if u.Scheme == "http" || u.Scheme == "https" {
		if strings.HasSuffix(u.Path, "jsonl") || strings.ToUpper(m.FileType) == "JSONL" {
			fmt.Println("Reading as a JSONL resource.")
			readJSONLResource(inputURL, recordchan)
		} else if strings.HasSuffix(u.Path, "gz") || strings.ToUpper(m.FileType) == "GZ" {
			fmt.Println("Reading as a GZ resource.")
			readGZResource(inputURL, recordchan)
		} else {
			fmt.Println("If this is a valid JSONL file, please rename with the .jsonl extension or use the file type override (--fileType).")
		}
	} else {
		msg := fmt.Sprintf("We don't handle %s input URLs.", u.Scheme)
		panic(msg)
	}
}

// ----------------------------------------------------------------------------

// process records in the JSONL format; reading one record per line from
// the given reader and placing the records into the record channel
func processJSONL(fileName string, reader io.Reader, recordchan chan queues.Record) {

	fmt.Println(time.Now(), "Start file read", fileName)

	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)

	i := 0
	for scanner.Scan() {
		i++
		str := strings.TrimSpace(scanner.Text())
		// ignore blank lines
		if len(str) > 0 {
			valid, err := record.Validate(str)
			if valid {
				recordchan <- &szRecord{str, i, fileName}
			} else {
				fmt.Println("Line", i, err)
			}
		}
		if i%10000 == 0 {
			fmt.Println(time.Now(), "Records sent to queue:", i)
		}
	}
	close(recordchan)

	fmt.Println(time.Now(), "Record channel close for file", fileName)
}

// ----------------------------------------------------------------------------

func readStdin(recordchan chan queues.Record) bool {
	info, err := os.Stdin.Stat()
	if err != nil {
		fmt.Println("Fatal error opening stdin.", err)
		return false
	}
	//printFileInfo(info)

	if info.Mode()&os.ModeNamedPipe == os.ModeNamedPipe {

		reader := bufio.NewReader(os.Stdin)
		processJSONL("stdin", reader, recordchan)
		return true
	}
	fmt.Println("Fatal error stdin not piped.")
	return false
}

// ----------------------------------------------------------------------------

// opens and reads a JSONL http resource
func readJSONLResource(jsonURL string, recordchan chan queues.Record) error {
	response, err := http.Get(jsonURL)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	processJSONL(jsonURL, response.Body, recordchan)
	return nil
}

// ----------------------------------------------------------------------------

// opens and reads a JSONL file
func readJSONLFile(jsonFile string, recordchan chan queues.Record) error {
	file, err := os.Open(jsonFile)
	if err != nil {
		return err
	}
	defer file.Close()

	processJSONL(jsonFile, file, recordchan)
	return nil
}

// ----------------------------------------------------------------------------

// opens and reads a JSONL file that has been Gzipped
func readGZFile(gzFile string, recordchan chan queues.Record) error {
	gzipfile, err := os.Open(gzFile)
	if err != nil {
		return err
	}
	defer gzipfile.Close()

	reader, err := gzip.NewReader(gzipfile)
	if err != nil {
		return err
	}
	defer reader.Close()

	processJSONL(gzFile, reader, recordchan)
	return nil
}

// ----------------------------------------------------------------------------
func readGZResource(gzURL string, recordchan chan queues.Record) error {
	response, err := http.Get(gzURL)
	if err != nil {
		fmt.Println("Fatal error retrieving inputURL.", err)
		return err
	}
	defer response.Body.Close()
	reader, err := gzip.NewReader(response.Body)
	if err != nil {
		fmt.Println("Fatal error reading inputURL.", err)
		return err
	}
	defer reader.Close()

	processJSONL(gzURL, reader, recordchan)
	return nil
}

// ----------------------------------------------------------------------------

// validates that a file is valid JSON
// TODO:  implement loading of a JSON file.  what is the actual JSON format?
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

	bytes := getBytes(file)
	if err := file.Close(); err != nil {
		log.Fatal(err)
	}

	valid := json.Valid(bytes)
	return valid
}

// ----------------------------------------------------------------------------

// used for validating a JSON file
// TODO:  this seems like a naive implementation.  What if the file is very large?
func getBytes(file *os.File) []byte {

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

// print basic file information.
// TODO:  should this info be logged?  DELETE ME?
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

// print out basic URL information.
// TODO:  should this info be logged?  DELETE ME?
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
// record implementation: provides a raw data record implementation
// ----------------------------------------------------------------------------

// Check at compile time that the implementation adheres to the interface.
var _ queues.Record = (*szRecord)(nil)

type szRecord struct {
	body   string
	id     int
	source string
}

func (r *szRecord) GetMessage() string {
	return r.body
}

func (r *szRecord) GetMessageId() string {
	//TODO: meaningful or random MessageId?
	return fmt.Sprintf("%s-%d", r.source, r.id)
}
