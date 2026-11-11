using BankingSample;

namespace EventSourcing.Tests;

public sealed class ProjectionTests
{
    private sealed class CountingProjection : IProjection
    {
        public int Count { get; private set; }

        public ValueTask ApplyAsync(EventEnvelope envelope, CancellationToken cancellationToken)
        {
            Count++;
            return ValueTask.CompletedTask;
        }
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Runner_applies_only_new_events_on_each_catch_up()
    {
        var store = new InMemoryEventStore();
        var repository = new AggregateRepository<BankAccount>(store);
        var balances = new AccountBalancesProjection();
        var runner = new ProjectionRunner(store, balances);

        var account = BankAccount.Open(Guid.NewGuid(), "Alice", "EUR");
        account.Deposit(100m);
        await repository.SaveAsync(account, Ct);
        Assert.Equal(2, await runner.CatchUpAsync(Ct));

        account.Deposit(50m);
        await repository.SaveAsync(account, Ct);
        Assert.Equal(1, await runner.CatchUpAsync(Ct));
        Assert.Equal(0, await runner.CatchUpAsync(Ct));

        var summary = balances.Find(account.Id);
        Assert.NotNull(summary);
        Assert.Equal(150m, summary.Balance);
        Assert.Equal(2, summary.TransactionCount);
        Assert.Equal(3, runner.Checkpoint);
    }

    [Fact]
    public async Task Runner_resumes_from_a_saved_checkpoint()
    {
        var store = new InMemoryEventStore();
        var repository = new AggregateRepository<BankAccount>(store);
        var account = BankAccount.Open(Guid.NewGuid(), "Alice", "EUR");
        account.Deposit(10m);
        await repository.SaveAsync(account, Ct);

        var firstRun = new ProjectionRunner(store, new CountingProjection());
        await firstRun.CatchUpAsync(Ct);

        account.Withdraw(5m);
        await repository.SaveAsync(account, Ct);

        var resumed = new CountingProjection();
        var applied = await new ProjectionRunner(store, resumed, firstRun.Checkpoint).CatchUpAsync(Ct);

        Assert.Equal(1, applied);
        Assert.Equal(1, resumed.Count);
    }
}
