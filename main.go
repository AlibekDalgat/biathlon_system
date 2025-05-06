package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type competitorStat struct {
	registered    bool
	startTime     time.Time
	actualStart   time.Time
	lapsTime      [][2]time.Time
	penaltyTime   [][2]time.Time
	hits          int
	notStarted    bool
	notFinished   bool
	finishTime    time.Time
	totalTime     time.Duration
	comment       string
	lapSpeeds     []float64
	penaltySpeeds []float64
}

var timeFormat = "15:04:05.000"

func main() {
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "15:04:05.000",
	})

	if err := initConfig(); err != nil {
		logrus.Fatalf("Ошибка инициализации конфигурации: %s", err.Error())
	}

	laps := viper.GetInt("laps")
	lapLen := viper.GetInt("lapLen")
	penaltyLen := viper.GetInt("penaltyLen")
	firingLines := viper.GetInt("firingLines")
	startStr := viper.GetString("start")
	startDeltaStr := viper.GetString("startDelta")

	start, err := time.Parse(timeFormat[:8], startStr)
	if err != nil {
		logrus.Fatalf("Ошибка парсинга времени старта: %s", err)
	}
	startDelta, err := time.Parse(timeFormat[:8], startDeltaStr)
	if err != nil {
		logrus.Fatalf("Ошибка парсинга времени интервала между стартами: %s", err)
	}

	fileIncomingEvents, err := os.Open("events")
	if err != nil {
		logrus.Fatalf("Ошибка открытия файла событий: %s", err)
	}
	defer fileIncomingEvents.Close()

	scanner := bufio.NewScanner(fileIncomingEvents)

	competitorsStats := make(map[string]*competitorStat)

	for scanner.Scan() {
		event := scanner.Text()
		err := handleEvent(event, competitorsStats, start, startDelta, laps)
		if err != nil {
			logrus.Fatal(err)
		}
	}

	if err := scanner.Err(); err != nil {
		logrus.Errorf("Ошибка чтения файла: %v", err)
	}

	fileResults, err := os.Create("resulting_table")
	if err != nil {
		logrus.Fatalf("Ошибка создания файла результатов: %s", err)
	}
	defer fileResults.Close()

	writeFinalReport(competitorsStats, fileResults, lapLen, penaltyLen, firingLines)
}

func handleEvent(event string, competitorStats map[string]*competitorStat, start time.Time, startDelta time.Time, laps int) error {
	params := strings.Split(event, " ")
	timeStr := params[0]
	idEvStr := params[1]
	idComp := params[2]

	timeEv, err := time.Parse(timeFormat, timeStr[1:len(timeStr)-1])
	if err != nil {
		return errors.New(fmt.Sprintf("Ошибка парсинга времени события: %s,  событие: %s", err, event))
	}

	idEv, err := strconv.Atoi(idEvStr)
	if err != nil {
		return errors.New(fmt.Sprintf("Ошибка преобразования ID события в число: %s, событие: %s", err, event))
	}

	if _, ok := competitorStats[idComp]; !ok {
		competitorStats[idComp] = &competitorStat{
			lapsTime:      make([][2]time.Time, 0),
			penaltyTime:   make([][2]time.Time, 0),
			hits:          0,
			lapSpeeds:     make([]float64, 0),
			penaltySpeeds: make([]float64, 0),
		}
	}

	stat := competitorStats[idComp]

	switch idEv {
	case 1: // Участник зарегистрирован
		stat.registered = true
		logrus.Infof("%s The competitor(%s) registered", timeStr, idComp)
	case 2: // Жеребьёвка старта
		startTimeStr := params[3]
		startTime, err := time.Parse(timeFormat, startTimeStr)
		if err != nil {
			return errors.New(fmt.Sprintf("Ошибка парсинга времени старта из события: %s, событие: %s", err, event))
		}
		stat.startTime = startTime
		logrus.Infof("%s The start time for the competitor(%s) was set by a draw to %s", timeStr, idComp, startTimeStr)
	case 3: // Участник на стартовой линии
		logrus.Infof("%s The competitor(%s) is on the start line", timeStr, idComp)
	case 4: // Участник стартовал
		stat.actualStart = timeEv
		stat.lapsTime = append(stat.lapsTime, [2]time.Time{timeEv})
		logrus.Infof("%s The competitor(%s) has started", timeStr, idComp)

		startDeltaDuration := time.Duration(startDelta.Hour())*time.Hour +
			time.Duration(startDelta.Minute())*time.Minute +
			time.Duration(startDelta.Second())*time.Second +
			time.Duration(startDelta.Nanosecond())*time.Nanosecond
		deadline := stat.startTime.Add(startDeltaDuration)
		if stat.actualStart.After(deadline) {
			stat.notStarted = true
			stat.comment = "Дисквалифицирован: старт после допустимого времени"
			logrus.Warnf("Участник %s дисквалифицирован: старт после допустимого времени (%s > %s).", idComp, stat.actualStart.Format(timeFormat), deadline.Format(timeFormat))
		}
	case 5: // Участник на огневом рубеже
		firingRange := params[3]
		logrus.Infof("%s The competitor(%s) is on the firing range(%s)", timeStr, idComp, firingRange)
	case 6: // Попадание в цель
		target := params[3]
		logrus.Infof("%s The target(%s) has been hit by competitor(%s)", timeStr, target, idComp)
		stat.hits++
	case 7: // Участник покинул огневой рубеж
		logrus.Infof("%s The competitor(%s) left the firing range", timeStr, idComp)
	case 8: // Участник зашел на штрафной круг
		stat.penaltyTime = append(stat.penaltyTime, [2]time.Time{timeEv, {}}) // Начало штрафного круга
		logrus.Infof("%s The competitor(%s) entered the penalty laps", timeStr, idComp)
	case 9: // Участник покинул штрафной круг
		stat.penaltyTime[len(stat.penaltyTime)-1][1] = timeEv // Конец штрафного круга
		logrus.Infof("%s The competitor(%s) left the penalty laps", timeStr, idComp)
	case 10: // Участник закончил круг
		stat.lapsTime[len(stat.lapsTime)-1][1] = timeEv
		logrus.Infof("%s The competitor(%s) ended the main lap", timeStr, idComp)
		if len(stat.lapsTime) < laps {
			stat.lapsTime = append(stat.lapsTime, [2]time.Time{timeEv})
		} else {
			stat.finishTime = timeEv
		}
	case 11: // Участник не может продолжать
		comment := strings.Join(params[3:], " ")
		stat.notFinished = true
		stat.comment = comment
		logrus.Infof("%s The competitor(%s) can`t continue: %s", timeStr, idComp, comment)

	default:
		logrus.Warnf("Неизвестный ID события: %s, событие: %s", idEvStr, event)
	}

	return nil
}

func writeFinalReport(competitorStats map[string]*competitorStat, file *os.File, lapLen, penaltyLen, firingLines int) {
	var competitorIDs []string
	for id := range competitorStats {
		competitorIDs = append(competitorIDs, id)
	}

	sort.Slice(competitorIDs, func(i, j int) bool {
		statI := competitorStats[competitorIDs[i]]
		statJ := competitorStats[competitorIDs[j]]
		if statI.notStarted {
			return true
		}
		if statJ.notStarted {
			return false
		}
		if statI.notFinished {
			return true
		}
		if statJ.notFinished {
			return false
		}

		return statI.finishTime.Sub(statI.actualStart) < statJ.finishTime.Sub(statJ.actualStart)
	})

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	for _, id := range competitorIDs {
		stat := competitorStats[id]

		var totalTimeStr string
		if stat.notStarted {
			totalTimeStr = "[NotStarted]"
		} else if stat.notFinished {
			totalTimeStr = "[NotFinished]"
		} else {
			totalTime := stat.finishTime.Sub(stat.actualStart)
			hours := int(totalTime.Hours())
			minutes := int(totalTime.Minutes()) % 60
			seconds := int(totalTime.Seconds()) % 60
			milliseconds := totalTime.Milliseconds() % 1000
			totalTimeStr = fmt.Sprintf("{%02d:%02d:%02d.%03d}", hours, minutes, seconds, milliseconds)
		}

		lapsTimeStr := "["
		for i, lap := range stat.lapsTime {
			if lap[0].IsZero() || lap[1].IsZero() {
				lapsTimeStr += "{,}"
			} else {
				lapTime := lap[1].Sub(lap[0])
				speed := float64(lapLen) / lapTime.Seconds()
				stat.lapSpeeds = append(stat.lapSpeeds, speed)
				hours := int(lapTime.Hours())
				minutes := int(lapTime.Minutes()) % 60
				seconds := int(lapTime.Seconds()) % 60
				milliseconds := lapTime.Milliseconds() % 1000

				lapsTimeStr += fmt.Sprintf("{%02d:%02d:%02d.%d, %.3f}", hours, minutes, seconds, milliseconds, speed)
			}
			if i < len(stat.lapsTime)-1 {
				lapsTimeStr += ", "
			}
		}
		lapsTimeStr += "]"

		penaltyTimeStr := "["
		for i, penalty := range stat.penaltyTime {
			if penalty[0].IsZero() || penalty[1].IsZero() {
				penaltyTimeStr += "{,}"
			} else {
				penaltyTime := penalty[1].Sub(penalty[0])
				speed := float64(penaltyLen) / penaltyTime.Seconds()
				stat.penaltySpeeds = append(stat.penaltySpeeds, speed)
				hours := int(penaltyTime.Hours())
				minutes := int(penaltyTime.Minutes()) % 60
				seconds := int(penaltyTime.Seconds()) % 60
				milliseconds := penaltyTime.Milliseconds() % 1000
				penaltyTimeStr += fmt.Sprintf("{%02d:%02d:%02d.%03d, %.3f}", hours, minutes, seconds, milliseconds, speed)
			}
			if i < len(stat.penaltyTime)-1 {
				penaltyTimeStr += ", "
			}
		}
		penaltyTimeStr += "]"

		resultString := fmt.Sprintf("%s %s %s %s %d/%d\n",
			totalTimeStr,
			id,
			lapsTimeStr,
			penaltyTimeStr,
			stat.hits,
			5*firingLines,
		)

		_, err := writer.WriteString(resultString)
		if err != nil {
			logrus.Errorf("Ошибка записи в файл: %s", err)
			return
		}
	}

}

func initConfig() error {
	viper.AddConfigPath("configs")
	viper.SetConfigName("config")
	viper.SetConfigType("json")

	return viper.ReadInConfig()
}
