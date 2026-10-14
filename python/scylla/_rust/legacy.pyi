class QueryExhausted(Exception):
    """
    Raised by `ResponseFuture.start_fetching_next_page()` when there are no more pages.
    """
