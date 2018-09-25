package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"gopkg.in/mgo.v2"
)

type (
	documentStruct = struct {
		FileName  string    ` bson:"fileName"`  // путь к файлу из которого получено сообщение
		LogTime   time.Time ` bson:"logTime"`   // время записи из лог файла
		LogMsg    string    ` bson:"logMsg"`    // текст сообщения из лог файла
		LogFormat string    ` bson:"logFormat"` // формат лога (first_format | second_format)
	}
)

const (
	firstFormat = iota + 1
	secondFormat
)

var (
	session *mgo.Session
	err     error
	dbhost  = "localhost"
)

func init() {

	session, err = mgo.Dial(dbhost)
	if err != nil {
		panic(err)
	}

	session.SetMode(mgo.Monotonic, true)

	go func() {
		for range time.Tick(time.Second * 1) {
			сonnectionCheck()
		}
	}()
}

func main() {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		fmt.Println("ERROR", err)
	}

	save := make(chan documentStruct)

	for _, file := range files {
		if !file.IsDir() && file.Name() != "main.go" {
			format := checkFormat(file.Name())
			go watch(file.Name(), format, save)
		}
	}

	for {
		saver(<-save)
	}
}

// watch создание рутины отслеживания изменений для файла
func watch(fileName string, format int, save chan documentStruct) {
	var watcher *fsnotify.Watcher
	watcher, _ = fsnotify.NewWatcher()
	defer watcher.Close()
	write := make(chan bool)
	go func() {
		skip := checkStartLen(fileName)
		for {
			select {
			case event := <-watcher.Events:
				time.Sleep(time.Second)
				if int(event.Op) == 2 {
					for _, val := range readNewLines(fileName, &skip) {
						save <- parseString(val, fileName, format)
					}
				}
			case err := <-watcher.Errors:
				fmt.Println("ERROR", err)
			}
		}
	}()
	if err := watcher.Add(fileName); err != nil {
		fmt.Println("ERROR", err)
	}
	<-write
}

//readNewLines чтение добавленых строк логов
func readNewLines(fileName string, skip *int) []string {
	inFile, err := os.Open(fileName)
	if err == nil {
		defer inFile.Close()
	} else {
		fmt.Println(err.Error() + `: ` + fileName)
		return nil
	}
	scanner := bufio.NewScanner(inFile)
	ret := []string{}
	i := 1
	for scanner.Scan() {
		if i > *skip {
			ret = append(ret, scanner.Text())
			*skip++
		}
		i++
	}
	return ret
}

//checkFormat проверка фомата логов в файле
func checkFormat(fileName string) int {
	inFile, err := os.Open(fileName)
	if err == nil {
		defer inFile.Close()
	} else {
		fmt.Println(err.Error() + `: ` + fileName)
		return 0
	}
	scanner := bufio.NewScanner(inFile)
	str := ""
	for scanner.Scan() {
		str = scanner.Text()
		break
	}
	if _, err := strconv.Atoi(str[:1]); err == nil {
		return secondFormat
	}
	return firstFormat
}

//checkStartLen Проверка на длину фйла при начале отслеживания
//В задаче не указана обработка Log Rotation , если необходимо, можно изменить функцию.
//Например для обнаружение даты в логе с предварительным получением из базу последнего.
func checkStartLen(fileName string) (len int) {
	inFile, err := os.Open(fileName)
	if err == nil {
		defer inFile.Close()
	} else {
		fmt.Println(err.Error() + `: ` + fileName)
		return 0
	}
	scanner := bufio.NewScanner(inFile)
	for scanner.Scan() {
		len++
	}
	return
}

//parseString формирование структуры из строки лога имени файла и формта
func parseString(oneString, fileName string, format int) documentStruct {
	stringArr := strings.Split(oneString, "|")
	logFormat := ""
	timeFormat := ""
	if format == firstFormat {
		logFormat = "first_format"
		timeFormat = "Jan 2, 2006 at 3:04:05pm (UTC)"
	} else {
		timeFormat = "2006-02-01T15:04:05Z"
		logFormat = "second_format"

	}
	parsedTime, err := time.Parse(timeFormat, strings.TrimSpace(stringArr[0]))
	if err != nil {
		fmt.Println("ERROR parse time", err)
	}
	return documentStruct{
		FileName:  fileName,
		LogTime:   parsedTime,
		LogMsg:    stringArr[1],
		LogFormat: logFormat,
	}
}

//saver функция обработчик канала, для записи в базу.
func saver(document documentStruct) {
	err := DB("newLogs").C("logs").Insert(document)
	if err != nil {
		fmt.Println("ERROR save to DB", err)
	}
}

// DB обертка для mgo.Session.DB
func DB(dname string) *mgo.Database {
	сonnectionCheck()
	return session.DB(dname)
}

// сonnectionCheck реконект при потере коннекта
func сonnectionCheck() {
	if err := session.Ping(); err != nil {
		fmt.Println("Lost connection to db!")
		session.Refresh()
		if err := session.Ping(); err == nil {
			fmt.Println("Reconnect to db successful.")
		}
	}
}
