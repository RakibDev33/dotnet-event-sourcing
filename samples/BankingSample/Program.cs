using BankingSample;
using EventSourcing;

var store = new InMemoryEventStore();
var accounts = new AggregateRepository<BankAccount>(store);

// 1. Behaviour produces events; the repository appends them to the account's stream.
var alice = BankAccount.Open(Guid.NewGuid(), "Alice", "EUR");
alice.Deposit(500m);
alice.Withdraw(120m);
await accounts.SaveAsync(alice);

var bob = BankAccount.Open(Guid.NewGuid(), "Bob", "EUR");
bob.Deposit(75m);
await accounts.SaveAsync(bob);

// 2. State is never stored, only rebuilt by replaying the stream.
var reloaded = await accounts.LoadAsync(alice.Id) ?? throw new InvalidOperationException("Account not found.");
Console.WriteLine($"Rebuilt {reloaded.Owner}'s account from {reloaded.Version} events: {reloaded.Balance:N2} {reloaded.Currency}");

// 3. Optimistic concurrency: two writers load the same version and both try to withdraw.
var firstWriter = await accounts.LoadAsync(alice.Id);
var secondWriter = await accounts.LoadAsync(alice.Id);
firstWriter!.Withdraw(300m);
secondWriter!.Withdraw(300m); // valid against the stale balance this writer loaded

await accounts.SaveAsync(firstWriter);
try
{
    await accounts.SaveAsync(secondWriter);
}
catch (ConcurrencyException conflict)
{
    Console.WriteLine($"Second writer rejected, overdraft prevented: {conflict.Message}");
}

// 4. Read models are projections over the global log and can be rebuilt at any time.
var balances = new AccountBalancesProjection();
var runner = new ProjectionRunner(store, balances);
var applied = await runner.CatchUpAsync();

Console.WriteLine($"Projection applied {applied} events (checkpoint {runner.Checkpoint}):");
foreach (var account in balances.Accounts.OrderBy(summary => summary.Owner))
{
    var transactions = account.TransactionCount == 1 ? "transaction" : "transactions";
    Console.WriteLine($"  {account.Owner,-6} {account.Balance,10:N2} {account.Currency}  ({account.TransactionCount} {transactions})");
}
