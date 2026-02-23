package hydro

// The implementation for the outbox goes here.
//
// This will allow Hydro to scale horizontally as well as keep transactionality on one node by:
// 1. Writing the change update to the database when it happens
// 2. Then taking it out again when we're sending it to pub/sub
//
// Important here:
// - The next event with the same identifier can only be processed when the last one was deleted (successfully sent through pub/sub)
// - We need a goroutine that pulls from the database every 100 milliseconds (or sth) and pushes stuff to pub/sub
