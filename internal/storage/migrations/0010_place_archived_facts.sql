-- Archived facts without a placement (PROTOCOL §6.10): rejected facts stored
-- before migration 0007 held no author position, so that migration did not place
-- them, yet a copy's fork may name one as its head. The Go step
-- placeArchivedFacts (migrate.go) places them as a push would, in receipt
-- order after every admitted fact, whose placement stays.
SELECT 1;
