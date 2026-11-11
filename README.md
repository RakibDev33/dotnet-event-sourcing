# dotnet-event-sourcing

![CI](https://github.com/RakibDev33/dotnet-event-sourcing/actions/workflows/ci.yml/badge.svg)

Event sourcing building blocks for **.NET 10** with no external dependencies, plus a banking sample that shows them working together:

- **`AggregateRoot`**: behaviour raises events, and a single `When` method applies them both live and during replay
- **`IEventStore`** with an in-memory implementation: per-stream versions, a dense global position, atomic appends and **optimistic concurrency**
- **`AggregateRepository<T>`**: rebuilds aggregates from their stream and saves new events against the expected version
- **`ProjectionRunner`**: catch-up subscription that feeds read models from a checkpoint, so projections can resume or be rebuilt

## Why event sourcing

Instead of storing the current balance of an account, the system stores what happened: `AccountOpened`, `MoneyDeposited`, `MoneyWithdrawn`. Current state is a left fold over those events. That gives a complete audit trail for free, makes it possible to build new read models from historical data, and turns concurrency conflicts into explicit, detectable errors instead of lost updates.

## Sample output

`samples/BankingSample` opens two accounts, rebuilds one from its stream, simulates two concurrent writers, and builds a balance read model:

```
$ dotnet run --project samples/BankingSample
Rebuilt Alice's account from 3 events: 380.00 EUR
Second writer rejected, overdraft prevented: Stream 3f2c... is at version 4, but version 3 was expected.
Projection applied 6 events (checkpoint 6):
  Alice       80.00 EUR  (3 transactions)
  Bob         75.00 EUR  (1 transaction)
```

Both writers loaded the account at version 3 with a balance of 380 EUR and each withdrew 300 EUR. Without the version check the second write would have silently overdrawn the account.

## Writing an aggregate

```csharp
public sealed class BankAccount : AggregateRoot
{
    public decimal Balance { get; private set; }

    public void Withdraw(decimal amount)
    {
        if (amount > Balance)
            throw new InvalidOperationException("Insufficient funds.");

        Raise(new MoneyWithdrawn(amount));   // validate first, then record the fact
    }

    protected override void When(IDomainEvent @event)
    {
        switch (@event)
        {
            case MoneyDeposited e: Balance += e.Amount; break;
            case MoneyWithdrawn e: Balance -= e.Amount; break;
            // ...
        }
    }
}
```

```csharp
var store = new InMemoryEventStore();
var accounts = new AggregateRepository<BankAccount>(store);

var account = await accounts.LoadAsync(id);
account!.Withdraw(50m);
await accounts.SaveAsync(account);   // throws ConcurrencyException if someone else wrote first
```

## Project structure

```
src/EventSourcing/          # Library: abstractions, aggregate base, repository, in-memory store, projections
samples/BankingSample/      # Bank account aggregate, balance projection and a runnable demo
tests/EventSourcing.Tests/  # xUnit v3 tests for the store contract, aggregate rules and projections
```

## Design notes

- **One code path for state changes.** `Raise` calls `When`, and replay calls `When`. Behaviour methods never assign state directly, so an aggregate rebuilt from history always matches the one that produced the events.
- **Events are facts.** `When` does not validate; validation happens before an event is raised. Replaying old events must never fail because a rule changed later.
- **Optimistic concurrency at the stream level.** `AppendAsync` takes the version the writer loaded. The check and the append happen under one lock in the in-memory store; a durable store would use a unique `(stream_id, version)` constraint or a conditional write.
- **Projections are disposable.** Read models are derived data. `ProjectionRunner` tracks a checkpoint, so a projection can resume after a restart or be rebuilt from position 0 when its shape changes.
- **Pluggable storage.** Anything that implements `IEventStore` (PostgreSQL, EventStoreDB, Cosmos DB) works with the same repository and projections.

## Running

Requires the .NET 10 SDK.

```bash
dotnet test
dotnet run --project samples/BankingSample
```

## License

MIT
