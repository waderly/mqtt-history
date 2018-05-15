package app

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo"
	"github.com/topfreegames/mqtt-history/logger"
)

// HistoriesHandler is the handler responsible for sending multiples rooms history to the player
func HistoriesHandler(app *App) func(c echo.Context) error {
	return func(c echo.Context) error {
		c.Set("route", "Histories")
		topicPrefix := c.ParamValues()[0]
		userID := c.QueryParam("userid")
		topicsSuffix := strings.Split(c.QueryParam("topics"), ",")
		topics := make([]string, len(topicsSuffix))
		from, err := strconv.Atoi(c.QueryParam("from"))
		limit, err := strconv.Atoi(c.QueryParam("limit"))
		for i, topicSuffix := range topicsSuffix {
			topics[i] = topicPrefix + "/" + topicSuffix
		}
		if limit == 0 {
			limit = 10
		}
		if from == 0 {
			from = int(time.Now().Unix())
		}

		logger.Logger.Debugf("user %s is asking for histories for topicPrefix %s with args topics=%s from=%d and limit=%d", userID, topicPrefix, topics, from, limit)
		authenticated, authorizedTopics, err := authenticate(c.StdContext(), app, userID, topics...)
		if err != nil {
			return err
		}

		if !authenticated {
			return c.String(echo.ErrUnauthorized.Code, echo.ErrUnauthorized.Message)
		}

		messages := app.Cassandra.SelectMessagesInTopics(c.StdContext(), authorizedTopics, int64(from), limit)

		return c.JSON(http.StatusOK, messages)
	}
}
