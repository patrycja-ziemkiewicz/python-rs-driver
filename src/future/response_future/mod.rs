//! `ResponseFuture`: the callback-driven future of the legacy driver.
//!
//! - `rows`: the rows of a page, built or still to build.
//! - `state`: the request in flight or how it settled, and the last page received.

#[expect(
    dead_code,
    reason = "ResponseFuture uses it; it lands in the next commit"
)]
mod rows;
#[expect(
    dead_code,
    reason = "ResponseFuture uses it; it lands in the next commit"
)]
mod state;
