package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/GoldenTypeAV/exchange-notifier/internal/exchanges"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

var bot *tgbotapi.BotAPI
var exchange exchanges.Exchange

func main() {
	initLogger()

	chatIdStr, exists := os.LookupEnv("TELEGRAM_CHAT_ID")

	if !exists || chatIdStr == "" {
		log.Panic("TELEGRAM_CHAT_ID is not defined")
	}

	var chatId int64
	var err error
	if chatId, err = strconv.ParseInt(chatIdStr, 10, 64); err != nil {
		log.Panic("TELEGRAM_CHAT_ID is not valid")
	}

	initTelegramBot()

	notifications := make(chan string, 100)
	initExchange(notifications)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := exchange.ConnectPrivateWS(ctx)
	if err != nil {
		log.Panic(err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "shutdown")

	exchange.SubscribePrivate(
		ctx,
		conn,
		exchanges.PrivateTopics{
			Orders:     false,
			Positions:  false,
			Wallet:     false,
			Executions: true,
		},
	)

	messages := make(chan json.RawMessage, 100)
	errChan := make(chan error, 1)

	go listenSocket(ctx, conn, messages, errChan)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	for {
		select {
		case msg := <-messages:
			exchange.HandleMessage(msg)

		case err := <-errChan:
			slog.Error(fmt.Sprintf("[Критическая ошибка] Сокет закрылся: %v\n", err))
			return

		case <-sigChan:
			slog.Info("[Система] Получен сигнал отмены. Завершаем работу...")
			return

		case notification := <-notifications:
			tmsg := tgbotapi.NewMessage(chatId, notification)
			bot.Send(tmsg) // TODO
		}
	}
}

func initLogger() {
	logLevel := slog.LevelInfo

	if debug, exists := os.LookupEnv("DEBUG"); exists && debug == "true" {
		logLevel = slog.LevelDebug
	}

	opts := &slog.HandlerOptions{
		Level: logLevel,
	}

	handler := slog.NewJSONHandler(os.Stdout, opts)
	logger := slog.New(handler)

	slog.SetDefault(logger)
}

// Инициализация бота
func initTelegramBot() {
	token, exists := os.LookupEnv("TELEGRAM_BOT_TOKEN")

	if !exists || token == "" {
		log.Panic("TELEGRAM_BOT_TOKEN is not defined")
	}

	var err error
	bot, err = tgbotapi.NewBotAPI(token)
	if err != nil {
		log.Panic(err)
	}

	if debug, exists := os.LookupEnv("DEBUG"); exists && debug == "true" {
		bot.Debug = true
	}

	slog.Info(fmt.Sprintf("Authorized on account %s", bot.Self.UserName))
}

// Инициализация клиента биржи
func initExchange(ch chan<- string) {
	exchange_name, exists := os.LookupEnv("EXCHANGE")

	if !exists || exchange_name == "" {
		log.Panic("EXCHANGE is not defined")
	}

	var err error
	if exchange, err = exchanges.New(exchange_name, ch); err != nil {
		log.Panic(err)
	}

	if err = exchange.CheckCredentials(); err != nil {
		log.Panic(err)
	}
}

// Слушатель сокета
func listenSocket(ctx context.Context, conn *websocket.Conn, messages chan<- json.RawMessage, errChan chan<- error) {
	slog.Info("[Горутина] Начат сбор данных из сокета...")

	for {
		var msg json.RawMessage
		if err := wsjson.Read(ctx, conn, &msg); err != nil {
			errChan <- err
			return
		}

		messages <- msg
	}
}
