package spectrestratum

import (
	"context"
	"fmt"
	"time"

	"github.com/GRinvestPOOL/spectre-stratum-bridge/src/gostratum"
	"github.com/pkg/errors"
	"github.com/spectre-network/spectred/app/appmessage"
	"github.com/spectre-network/spectred/infrastructure/network/rpcclient"
	"go.uber.org/zap"
)

type spectreApi struct {
	address       string
	blockWaitTime time.Duration
	logger        *zap.SugaredLogger
	spectred      *rpcclient.RPCClient
	connected     bool
}

func NewspectreApi(address string, blockWaitTime time.Duration, logger *zap.SugaredLogger) (*spectreApi, error) {
	client, err := rpcclient.NewRPCClient(address)
	if err != nil {
		return nil, err
	}

	return &spectreApi{
		address:       address,
		blockWaitTime: blockWaitTime,
		logger:        logger.With(zap.String("component", "spectreapi:"+address)),
		spectred:      client,
		connected:     true,
	}, nil
}

func (ks *spectreApi) Start(ctx context.Context, blockCb func()) {
	ks.waitForSync(true)
	go ks.startBlockTemplateListener(ctx, blockCb)
	go ks.startStatsThread(ctx)
}

func (ks *spectreApi) startStatsThread(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			ks.logger.Warn("context cancelled, stopping stats thread")
			return
		case <-ticker.C:
			dagResponse, err := ks.spectred.GetBlockDAGInfo()
			if err != nil {
				ks.logger.Warn("failed to get network hashrate from spectre, prom stats will be out of date", zap.Error(err))
				continue
			}
			response, err := ks.spectred.EstimateNetworkHashesPerSecond(dagResponse.TipHashes[0], 1000)
			if err != nil {
				ks.logger.Warn("failed to get network hashrate from spectre, prom stats will be out of date", zap.Error(err))
				continue
			}
			RecordNetworkStats(response.NetworkHashesPerSecond, dagResponse.BlockCount, dagResponse.Difficulty)
		}
	}
}

func (ks *spectreApi) reconnect() error {
	if ks.spectred != nil {
		return ks.spectred.Reconnect()
	}

	client, err := rpcclient.NewRPCClient(ks.address)
	if err != nil {
		return err
	}
	ks.spectred = client
	return nil
}

func (s *spectreApi) waitForSync(verbose bool) error {
	if verbose {
		s.logger.Info("checking spectred sync state")
	}
	for {
		clientInfo, err := s.spectred.GetInfo()
		if err != nil {
			return errors.Wrapf(err, "error fetching server info from spectred @ %s", s.address)
		}
		if clientInfo.IsSynced {
			break
		}
		s.logger.Warn("spectre is not synced, waiting for sync before starting bridge")
		time.Sleep(5 * time.Second)
	}
	if verbose {
		s.logger.Info("spectred synced, starting server")
	}
	return nil
}

func (s *spectreApi) startBlockTemplateListener(ctx context.Context, blockReadyCb func()) {
	blockReadyChan := make(chan bool)
	err := s.spectred.RegisterForNewBlockTemplateNotifications(func(_ *appmessage.NewBlockTemplateNotificationMessage) {
		blockReadyChan <- true
	})
	if err != nil {
		s.logger.Error("fatal: failed to register for block notifications from spectre")
	}

	ticker := time.NewTicker(s.blockWaitTime)
	for {
		if err := s.waitForSync(false); err != nil {
			s.logger.Error("error checking spectred sync state, attempting reconnect: ", err)
			if err := s.reconnect(); err != nil {
				s.logger.Error("error reconnecting to spectred, waiting before retry: ", err)
				time.Sleep(5 * time.Second)
			}
		}
		select {
		case <-ctx.Done():
			s.logger.Warn("context cancelled, stopping block update listener")
			return
		case <-blockReadyChan:
			blockReadyCb()
			ticker.Reset(s.blockWaitTime)
		case <-ticker.C: // timeout, manually check for new blocks
			blockReadyCb()
		}
	}
}

func (ks *spectreApi) GetBlockTemplate(
	client *gostratum.StratumContext) (*appmessage.GetBlockTemplateResponseMessage, error) {
	template, err := ks.spectred.GetBlockTemplate(client.WalletAddr,
		fmt.Sprintf(`'%s' via spectre-network/spectre-stratum-bridge_%s`, client.RemoteApp, version))
	if err != nil {
		return nil, errors.Wrap(err, "failed fetching new block template from spectre")
	}
	return template, nil
}
