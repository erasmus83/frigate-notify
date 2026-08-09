package events

import (
	"sync"
	"time"

	"github.com/maypok86/otter"
	"github.com/rs/zerolog/log"

	"github.com/0x2142/frigate-notify/config"
	"github.com/0x2142/frigate-notify/models"
	"github.com/0x2142/frigate-notify/notifier"
)

// reviewState tracks the in-progress alert for a single review ID, so that a later
// "update"/"genai"/"end" MQTT message can complete it without re-deciding whether to alert.
type reviewState struct {
	mu         sync.Mutex
	Detections []models.Event
	GenAISent  bool
}

var reviewStateCache otter.Cache[string, *reviewState]

// InitReviewStateCache sets up the review state cache, used to track in-progress GenAI alerts
func InitReviewStateCache() {
	var err error
	log.Debug().Msg("Setting up review state cache...")
	reviewStateCache, err = otter.MustBuilder[string, *reviewState](500).WithTTL(2 * time.Hour).Build()
	if err != nil {
		log.Warn().
			Err(err).
			Msg("Error setting up review state cache")
	}
	log.Debug().Msg("Review state cache ready")
}

// CloseReviewStateCache tears down the review state cache
func CloseReviewStateCache() {
	log.Debug().Msg("Review state cache tear down")
	reviewStateCache.Close()
}

func getReviewState(id string) (*reviewState, bool) {
	return reviewStateCache.Get(id)
}

func setReviewState(id string, state *reviewState) {
	reviewStateCache.Set(id, state)
}

func deleteReviewState(id string) {
	reviewStateCache.Delete(id)
}

// handleReviewNew sends the initial, short-TTL alert for a review & tracks it so a later
// genai/end message can complete it
func handleReviewNew(review models.Review) {
	if config.ConfigData.Alerts.General.RecheckDelay != 0 {
		review = recheckReview(review)
	}

	events, ok := buildReviewAlert(review)
	if !ok {
		return
	}

	for i := range events {
		events[i].Extra.AlertTTL = config.ConfigData.Alerts.General.GenAIInitialTTL
	}

	setReviewState(review.ID, &reviewState{Detections: events})

	notifier.SendAlert(events)
}

// handleReviewUpdate refreshes the cached detection details for an in-progress review.
// No alert is sent - this only keeps data current for whenever the follow-up alert goes out.
func handleReviewUpdate(review models.Review) {
	state, ok := getReviewState(review.ID)
	if !ok {
		log.Debug().
			Str("review_id", review.ID).
			Msg("Review update dropped - No active alert for this review")
		return
	}

	events := fetchDetections(review)
	if len(events) == 0 {
		return
	}

	state.mu.Lock()
	state.Detections = events
	state.mu.Unlock()

	log.Debug().
		Str("review_id", review.ID).
		Msg("Review update - Cached detection details refreshed")
}

// handleReviewGenAI sends the long-TTL follow-up alert once Frigate's Generative AI
// description is available
func handleReviewGenAI(review models.Review) {
	state, ok := getReviewState(review.ID)
	if !ok {
		log.Debug().
			Str("review_id", review.ID).
			Msg("GenAI event dropped - No active alert for this review")
		return
	}

	events := fetchDetections(review)
	if len(events) == 0 {
		log.Warn().
			Str("review_id", review.ID).
			Msg("GenAI event dropped - No detections available to alert on")
		return
	}

	for i := range events {
		applyGenAIMetadata(&events[i].Extra, review.Data.Metadata)
		events[i].Extra.AlertTTL = config.ConfigData.Alerts.General.GenAIFinalTTL
	}

	state.mu.Lock()
	state.Detections = events
	state.GenAISent = true
	state.mu.Unlock()

	log.Info().
		Str("review_id", review.ID).
		Msg("GenAI description received - Sending follow-up alert")

	notifier.SendAlert(events)
}

// handleReviewEnd clears the per-detection zone cache immediately, then schedules a delayed
// check: if no GenAI description arrived within genai_end_delay, send the follow-up alert
// anyway (without the description) before clearing the review's cached state.
func handleReviewEnd(review models.Review) {
	for _, detection := range review.Data.Detections {
		delZoneAlerted(models.Event{
			ID:           detection,
			Camera:       review.Camera,
			CurrentZones: review.Data.Zones,
		})
	}

	if _, ok := getReviewState(review.ID); !ok {
		return
	}

	delay := time.Duration(config.ConfigData.Alerts.General.GenAIEndDelay) * time.Second
	log.Debug().
		Str("review_id", review.ID).
		Dur("delay", delay).
		Msg("Review ended - Scheduling GenAI fallback check")

	time.AfterFunc(delay, func() {
		state, ok := getReviewState(review.ID)
		if !ok {
			return
		}

		state.mu.Lock()
		alreadySent := state.GenAISent
		events := state.Detections
		state.mu.Unlock()

		if !alreadySent {
			log.Info().
				Str("review_id", review.ID).
				Msg("No GenAI description received - Sending fallback alert")
			for i := range events {
				events[i].Extra.AlertTTL = config.ConfigData.Alerts.General.GenAIFinalTTL
			}
			notifier.SendAlert(events)
		}

		deleteReviewState(review.ID)
	})
}
