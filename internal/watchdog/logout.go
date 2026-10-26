package watchdog

import (
	"context"
	"time"

	"integration-xcpng/internal/xapi"
)

// LogOut closes a watchdog's session, with a few seconds of its own: ctx may be over
// already, the watchdog's time spent on an object xapi kept. Its error is dropped, as
// the session goes with the process anyway and xapi ends it in time.
func LogOut(ctx context.Context, client *xapi.Client) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), logoutTimeout)
	defer cancel()
	_ = client.Close(ctx)
}

// logoutTimeout bounds a watchdog's logout.
const logoutTimeout = 5 * time.Second
