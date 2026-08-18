package types

import (
	"time"

	"bitbucket.org/zapspace/zap-go-server/config/constants"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Order represents an order entity
type Order struct {
	ID                           primitive.ObjectID     `bson:"_id,omitempty"`
	Platform                     string                 `bson:"platform"`
	V1UserID                     primitive.ObjectID     `bson:"v1userId,omitempty"`
	UserID                       primitive.ObjectID     `bson:"userId"`
	Type                         string                 `bson:"type"`
	Flow                         string                 `bson:"flow"`
	CurrencyID                   primitive.ObjectID     `bson:"currencyId"`
	BuyChain                     primitive.ObjectID     `bson:"buyChain,omitempty"`
	SellChain                    primitive.ObjectID     `bson:"sellChain,omitempty"`
	OpenAmount                   float64                `bson:"openAmount"`
	Amount                       float64                `bson:"amount"`
	RefundAmount                 float64                `bson:"refundAmount"`
	CalculatedAmount             float64                `bson:"calculatedAmount"`
	OpenRate                     float64                `bson:"openRate"`
	Rate                         float64                `bson:"rate"`
	CalculatedRate               float64                `bson:"calculatedRate"`
	ProviderFeesPaid             float64                `bson:"providerFeesPaid"`
	BlockchainFeesPaid           float64                `bson:"blockchainFeesPaid"`
	FeesGained                   float64                `bson:"feesGained"`
	ProviderFeesToPay            float64                `bson:"providerFeesToPay"`
	BlockchainFeesToPay          float64                `bson:"blockchainFeesToPay"`
	FeesToGain                   float64                `bson:"feesToGain"`
	CalculatedProviderFeesPaid   float64                `bson:"calculatedProviderFeesPaid"`
	CalculatedBlockchainFeesPaid float64                `bson:"calculatedBlockchainFeesPaid"`
	CalculatedFeesGained         float64                `bson:"calculatedFeesGained"`
	Status                       string                 `bson:"status"`
	StatusTimestamps             map[string]interface{} `bson:"statusTimestamps"`
	V1ID                         string                 `bson:"v1Id,omitempty"`
	DepositAccountIDs            []primitive.ObjectID   `bson:"depositAccountIds"`
	WithdrawalAccountIDs         []primitive.ObjectID   `bson:"withdrawalAccountIds"`
	RefundAccountID              primitive.ObjectID     `bson:"refundAccountId,omitempty"`
	TransactionIDs               []primitive.ObjectID   `bson:"transactionIds"`
	TransactionTypes             []string               `bson:"transactionTypes"`
	ParentOrderID                primitive.ObjectID     `bson:"parentOrder,omitempty"`
	ChildOrderID                 primitive.ObjectID     `bson:"childOrder,omitempty"`
	Provider                     string                 `bson:"provider"`
	ProviderOrderID              string                 `bson:"providerOrderId,omitempty"`
	BuyProvider                  string                 `bson:"buyProvider,omitempty"`
	SellProvider                 string                 `bson:"sellProvider,omitempty"`
	Margin                       float64                `bson:"margin"`
	RateID                       string                 `bson:"rateId,omitempty"`
	ExpiresAt                    time.Time              `bson:"expiresAt"`
	DeletedAt                    *time.Time             `bson:"deletedAt,omitempty"`
	CreatedAt                    time.Time              `bson:"createdAt"`
	UpdatedAt                    time.Time              `bson:"updatedAt"`
}

// NewOrder creates a new order instance
func NewOrder(
	userID, currencyID primitive.ObjectID,
	flow string,
	amount, rate float64,
) *Order {
	now := time.Now()
	return &Order{
		ID:                   primitive.NewObjectID(),
		UserID:               userID,
		CurrencyID:           currencyID,
		Flow:                 flow,
		Amount:               amount,
		OpenAmount:           amount,
		Rate:                 rate,
		OpenRate:             rate,
		Status:               constants.ORDER_STATUS_CREATED,
		StatusTimestamps:     map[string]interface{}{constants.ORDER_STATUS_CREATED: now.Unix()},
		DepositAccountIDs:    []primitive.ObjectID{},
		WithdrawalAccountIDs: []primitive.ObjectID{},
		TransactionIDs:       []primitive.ObjectID{},
		TransactionTypes:     []string{},
		Platform:             "APP",
		CreatedAt:            now,
		UpdatedAt:            now,
	}
}

// MarkAsDeleted marks the order as deleted
func (o *Order) MarkAsDeleted() {
	now := time.Time{}
	o.DeletedAt = &now
	o.UpdatedAt = now
}

// UpdateStatus updates the order status and timestamps
func (o *Order) UpdateStatus(status string) {
	now := time.Now()
	o.Status = status
	if o.StatusTimestamps == nil {
		o.StatusTimestamps = make(map[string]interface{})
	}
	o.StatusTimestamps[status] = now.Unix()
	o.UpdatedAt = now
}
