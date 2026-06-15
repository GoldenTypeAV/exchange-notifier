package exchanges

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type Bybit struct {
	Channel   chan<- string
	APIKey    string
	APISecret string
}

func (b *Bybit) CheckCredentials() error {
	var ok bool

	b.APIKey, ok = os.LookupEnv("BYBIT_API_KEY")
	if !ok || b.APIKey == "" {
		return fmt.Errorf("BYBIT_API_KEY not found")
	}

	b.APISecret, ok = os.LookupEnv("BYBIT_API_SECRET")
	if !ok || b.APISecret == "" {
		return fmt.Errorf("BYBIT_API_SECRET not found")
	}

	return nil
}

func (b *Bybit) keepAlive(ctx context.Context, conn *websocket.Conn) {
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			pingMessage := []byte(`{"op":"ping"}`)
			if err := conn.Write(ctx, websocket.MessageText, pingMessage); err != nil {
				slog.Error(fmt.Sprintf("Ошибка отправки ping: %v", err))
				return
			}
			slog.Debug("Ping отправлен успешно")
			timer.Reset(20 * time.Second)
		}
	}
}

func (b *Bybit) ConnectPublicWS(ctx context.Context) (*websocket.Conn, error) {
	conn, _, err := websocket.Dial(
		ctx,
		"wss://stream.bybit.com/v5/public/spot", // wss://stream.bybit.com/v5/public/linear
		nil,
	)
	return conn, err
}

func (b *Bybit) authenticate(ctx context.Context, conn *websocket.Conn) error {
	expires := time.Now().UnixMilli() + 10000
	payload := fmt.Sprintf("GET/realtime%d", expires)

	mac := hmac.New(
		sha256.New,
		[]byte(b.APISecret),
	)

	mac.Write([]byte(payload))

	signature := hex.EncodeToString(mac.Sum(nil))

	auth := map[string]any{
		"op": "auth",
		"args": []any{
			b.APIKey,
			expires,
			signature,
		},
	}

	err := wsjson.Write(ctx, conn, auth)
	return err
}

func (b *Bybit) ConnectPrivateWS(ctx context.Context) (*websocket.Conn, error) {
	conn, _, err := websocket.Dial(
		ctx,
		//"wss://stream-demo.bybit.com/v5/private",
		"wss://stream.bybit.com/v5/private",
		nil,
	)
	if err != nil {
		return nil, err
	}

	if err := b.authenticate(ctx, conn); err != nil {
		conn.Close(
			websocket.StatusInternalError,
			"auth failed",
		)
		return nil, err
	}

	go b.keepAlive(ctx, conn)

	return conn, nil
}

func (b *Bybit) SubscribePrivate(
	ctx context.Context,
	conn *websocket.Conn,
	topics PrivateTopics,
) error {
	var args []string

	if topics.Orders == true {
		args = append(args, "order")
	}

	if topics.Executions == true {
		args = append(args, "execution")
	}

	if topics.Positions == true {
		args = append(args, "position")
	}

	if topics.Wallet == true {
		args = append(args, "wallet")
	}

	req := struct {
		Op   string   `json:"op"`
		Args []string `json:"args"`
	}{
		Op:   "subscribe",
		Args: args,
	}

	return wsjson.Write(ctx, conn, req)
}

type bybitMessage struct {
	Op      string          `json:"op"`
	Success bool            `json:"success"`
	RetMsg  string          `json:"ret_msg"`
	Topic   string          `json:"topic"`
	Data    json.RawMessage `json:"data"`
}

type bybitPosition struct {
}

type bybitOrder struct {
	Category              string          `json:"category"`
	Symbol                string          `json:"symbol"`
	OrderId               string          `json:"orderId"`
	OrderLinkId           string          `json:"orderLinkId"`
	BlockTradeId          string          `json:"blockTradeId"`
	Side                  string          `json:"side"`
	PositionIdx           int64           `json:"positionIdx"`
	OrderStatus           string          `json:"orderStatus"`
	CancelType            string          `json:"cancelType"`
	RejectReason          string          `json:"rejectReason"`
	TimeInForce           string          `json:"timeInForce"`
	IsLeverage            string          `json:"isLeverage"`
	Price                 string          `json:"price"`
	Qty                   string          `json:"qty"`
	AvgPrice              string          `json:"avgPrice"`
	LeavesQty             string          `json:"leavesQty"`
	LeavesValue           string          `json:"leavesValue"`
	CumExecQty            string          `json:"cumExecQty"`
	CumExecValue          string          `json:"cumExecValue"`
	CumExecFee            string          `json:"cumExecFee"`
	OrderType             string          `json:"orderType"`
	StopOrderType         string          `json:"stopOrderType"`
	OrderIv               string          `json:"orderIv"`
	TriggerPrice          string          `json:"triggerPrice"`
	TakeProfit            string          `json:"takeProfit"`
	StopLoss              string          `json:"stopLoss"`
	TriggerBy             string          `json:"triggerBy"`
	TpTriggerBy           string          `json:"tpTriggerBy"`
	SlTriggerBy           string          `json:"slTriggerBy"`
	TriggerDirection      int64           `json:"triggerDirection"`
	PlaceType             string          `json:"placeType"`
	LastPriceOnCreated    string          `json:"lastPriceOnCreated"`
	CloseOnTrigger        bool            `json:"closeOnTrigger"`
	ReduceOnly            bool            `json:"reduceOnly"`
	SmpGroup              int64           `json:"smpGroup"`
	SmpType               string          `json:"smpType"`
	SmpOrderId            string          `json:"smpOrderId"`
	SlLimitPrice          string          `json:"slLimitPrice"`
	TpLimitPrice          string          `json:"tpLimitPrice"`
	MarketUnit            string          `json:"marketUnit"`
	CreatedTime           string          `json:"createdTime"`
	UpdatedTime           string          `json:"updatedTime"`
	FeeCurrency           string          `json:"feeCurrency"`
	SlippageTolerance     string          `json:"slippageTolerance"`
	SlippageToleranceType string          `json:"slippageToleranceType"`
	CumFeeDetail          json.RawMessage `json:"cumFeeDetail"`
	RpiTakerAccess        bool            `json:"rpiTakerAccess"`
	RpiMatchedQty         string          `json:"rpiMatchedQty"`
}

type bybitCoin struct {
	Coin                string `json:"coin"`
	Equity              string `json:"equity"`
	UsdValue            string `json:"usdValue"`
	WalletBalance       string `json:"walletBalance"`
	AvailableToWithdraw string `json:"availableToWithdraw"`
	AvailableToBorrow   string `json:"availableToBorrow"`
	BorrowAmount        string `json:"borrowAmount"`
	AccruedInterest     string `json:"accruedInterest"`
	TotalOrderIM        string `json:"totalOrderIM"`
	TotalPositionIM     string `json:"totalPositionIM"`
	TotalPositionMM     string `json:"totalPositionMM"`
	UnrealisedPnl       string `json:"unrealisedPnl"`
	CumRealisedPnl      string `json:"cumRealisedPnl"`
	Bonus               string `json:"bonus"`
	CollateralSwitch    bool   `json:"collateralSwitch"`
	MarginCollateral    bool   `json:"marginCollateral"`
	Locked              string `json:"locked"`
	SpotHedgingQty      string `json:"spotHedgingQty"`
	SpotBorrow          string `json:"spotBorrow"`
	ColRes              string `json:"colRes"`
}

type bybitWallet struct {
	AccountIMRate              string      `json:"accountIMRate"`
	AccountMMRate              string      `json:"accountMMRate"`
	AccountIMRateByMp          string      `json:"accountIMRateByMp"`
	AccountMMRateByMp          string      `json:"accountMMRateByMp"`
	TotalEquity                string      `json:"totalEquity"`
	TotalWalletBalance         string      `json:"totalWalletBalance"`
	TotalMarginBalance         string      `json:"totalMarginBalance"`
	TotalAvailableBalance      string      `json:"totalAvailableBalance"`
	TotalPerpUPL               string      `json:"totalPerpUPL"`
	TotalInitialMargin         string      `json:"totalInitialMargin"`
	TotalMaintenanceMargin     string      `json:"totalMaintenanceMargin"`
	TotalInitialMarginByMp     string      `json:"totalInitialMarginByMp"`
	TotalMaintenanceMarginByMp string      `json:"totalMaintenanceMarginByMp"`
	Coin                       []bybitCoin `json:"coin"`
	AccountLTV                 string      `json:"accountLTV"`
	AccountType                string      `json:"accountType"`
}

type bybitExecution struct {
	Category        string          `json:"category"`
	Symbol          string          `json:"symbol"`
	ClosedSize      string          `json:"closedSize"`
	ExecFee         string          `json:"execFee"`
	ExecId          string          `json:"execId"`
	ExecPrice       string          `json:"execPrice"`
	ExecQty         string          `json:"execQty"`
	ExecType        string          `json:"execType"`
	ExecValue       string          `json:"execValue"`
	FeeRate         string          `json:"feeRate"`
	TradeIv         string          `json:"tradeIv"`
	MarkIv          string          `json:"markIv"`
	BlockTradeId    string          `json:"blockTradeId"`
	MarkPrice       string          `json:"markPrice"`
	IndexPrice      string          `json:"indexPrice"`
	UnderlyingPrice string          `json:"underlyingPrice"`
	LeavesQty       string          `json:"leavesQty"`
	OrderId         string          `json:"orderId"`
	OrderLinkId     string          `json:"orderLinkId"`
	OrderPrice      string          `json:"orderPrice"`
	OrderQty        string          `json:"orderQty"`
	OrderType       string          `json:"orderType"`
	StopOrderType   string          `json:"stopOrderType"`
	Side            string          `json:"side"`
	ExecTime        string          `json:"execTime"`
	IsLeverage      string          `json:"isLeverage"`
	IsMaker         bool            `json:"isMaker"`
	Seq             int64           `json:"seq"`
	MarketUnit      string          `json:"marketUnit"`
	ExecPnl         string          `json:"execPnl"`
	CreateType      string          `json:"createType"`
	ExtraFees       json.RawMessage `json:"extraFees"`
	FeeCurrency     string          `json:"feeCurrency"`
}

func (b *Bybit) HandleMessage(msg json.RawMessage) {
	var m bybitMessage
	if err := json.Unmarshal(msg, &m); err != nil {
		slog.Warn(fmt.Sprintf("bybit: не удалось разобрать сообщение: %v\n", err))
		return
	}

	slog.Debug(string(msg))

	switch {
	case m.Op == "subscribe":
		if m.Success {
			slog.Info("Подписка на канал успешна")
		} else {
			slog.Error(fmt.Sprintf("Подписка не удалась: %s\n", m.RetMsg))
		}
	case m.Op == "auth":
		if m.Success {
			slog.Info("Успешная авторизация")
		} else {
			slog.Error(fmt.Sprintf("Ошибка авторизации: %s\n", m.RetMsg))
		}
	case m.Op == "pong":
		slog.Debug("Получен pong")
		return
	case m.Topic == "position":
		var d []bybitPosition
		if err := json.Unmarshal(m.Data, &d); err != nil {
			slog.Warn(fmt.Sprintf("bybit: не удалось обработать position: %v\n", err))
			return
		}
		b.handlePosition(d)
	case m.Topic == "wallet":
		var d []bybitWallet
		if err := json.Unmarshal(m.Data, &d); err != nil {
			slog.Warn(fmt.Sprintf("bybit: не удалось обработать wallet: %v\n", err))
			return
		}
		b.handleWallet(d)
	case m.Topic == "execution":
		var d []bybitExecution
		if err := json.Unmarshal(m.Data, &d); err != nil {
			slog.Warn(fmt.Sprintf("bybit: не удалось обработать execution: %v\n", err))
			return
		}
		b.handleExecution(d)
	case m.Topic == "order":
		var d []bybitOrder
		if err := json.Unmarshal(m.Data, &d); err != nil {
			slog.Warn(fmt.Sprintf("bybit: не удалось обработать order: %v\n", err))
			return
		}
		b.handleOrder(d)
	default:
		fmt.Println(string(msg))
	}
}

func (b *Bybit) handlePosition(data []bybitPosition) {}

func (b *Bybit) handleOrder(data []bybitOrder) {
	statuses := []string{"Filled", "PartiallyFilled"}
	for _, d := range data {
		if !slices.Contains(statuses, d.OrderStatus) {
			continue
		}

		action := "Покупка 🟢"
		if d.Side == "Sell" {
			action = "Продажа 🔴"
		}

		execPrice, _ := strconv.ParseFloat(d.Price, 64)
		execValue, _ := strconv.ParseFloat(d.CumExecValue, 64)
		execFee, _ := strconv.ParseFloat(d.CumExecFee, 64)

		b.Channel <- fmt.Sprintf(
			"📊 **Исполнение ордера (%s)**\n"+
				"• Тип: %s\n"+
				"• Биржа: Bybit\n"+
				"• Пара: %s\n"+
				"• Цена: %.2f\n"+
				"• Кол-во: %s\n"+
				"• Всего: %.2f %s\n"+
				"• Комиссия: %.2f %s",
			strings.ToUpper(d.Category),
			action,
			d.Symbol,
			execPrice,
			d.Qty,
			execValue, d.FeeCurrency,
			execFee, d.FeeCurrency,
		)
	}
}

func (b *Bybit) handleWallet(data []bybitWallet) {}

func (b *Bybit) handleExecution(data []bybitExecution) {
	for _, d := range data {
		if d.ExecType != "Trade" {
			continue
		}

		action := "Покупка 🟢"
		if d.Side == "Sell" {
			action = "Продажа 🔴"
		}

		execPrice, _ := strconv.ParseFloat(d.ExecPrice, 64)
		execValue, _ := strconv.ParseFloat(d.ExecValue, 64)
		execFee, _ := strconv.ParseFloat(d.ExecFee, 64)

		b.Channel <- fmt.Sprintf(
			"📊 **Исполнение ордера (%s)**\n"+
				"• Тип: %s\n"+
				"• Биржа: Bybit\n"+
				"• Пара: %s\n"+
				"• Цена: %.2f\n"+
				"• Кол-во: %s\n"+
				"• Всего: %.2f %s\n"+
				"• Комиссия: %.2f %s",
			strings.ToUpper(d.Category),
			action,
			d.Symbol,
			execPrice,
			d.ExecQty,
			execValue, d.FeeCurrency,
			execFee, d.FeeCurrency,
		)
	}
}
