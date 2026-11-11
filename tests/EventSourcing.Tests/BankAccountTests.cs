using BankingSample;

namespace EventSourcing.Tests;

public sealed class BankAccountTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void Behaviour_raises_events_and_updates_state()
    {
        var account = BankAccount.Open(Guid.NewGuid(), " Alice ", "eur");

        account.Deposit(100m);
        account.Withdraw(30m);

        Assert.Equal("Alice", account.Owner);
        Assert.Equal("EUR", account.Currency);
        Assert.Equal(70m, account.Balance);
        Assert.Collection(
            account.UncommittedEvents,
            e => Assert.IsType<AccountOpened>(e),
            e => Assert.Equal(new MoneyDeposited(100m), e),
            e => Assert.Equal(new MoneyWithdrawn(30m), e));
    }

    [Fact]
    public void Overdraft_is_rejected_without_raising_an_event()
    {
        var account = BankAccount.Open(Guid.NewGuid(), "Alice", "EUR");
        account.Deposit(50m);

        Assert.Throws<InvalidOperationException>(() => account.Withdraw(80m));

        Assert.Equal(50m, account.Balance);
        Assert.Equal(2, account.UncommittedEvents.Count);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-10)]
    public void Amounts_must_be_positive(int amount)
    {
        var account = BankAccount.Open(Guid.NewGuid(), "Alice", "EUR");

        Assert.Throws<ArgumentOutOfRangeException>(() => account.Deposit(amount));
    }

    [Fact]
    public void Only_empty_accounts_can_be_closed_and_closed_accounts_reject_deposits()
    {
        var account = BankAccount.Open(Guid.NewGuid(), "Alice", "EUR");
        account.Deposit(10m);
        Assert.Throws<InvalidOperationException>(account.Close);

        account.Withdraw(10m);
        account.Close();

        Assert.True(account.IsClosed);
        Assert.Throws<InvalidOperationException>(() => account.Deposit(1m));
    }

    [Fact]
    public async Task Repository_rebuilds_state_and_version_from_the_stream()
    {
        var repository = new AggregateRepository<BankAccount>(new InMemoryEventStore());
        var account = BankAccount.Open(Guid.NewGuid(), "Alice", "EUR");
        account.Deposit(200m);
        account.Withdraw(50m);
        await repository.SaveAsync(account, Ct);

        var loaded = await repository.LoadAsync(account.Id, Ct);

        Assert.NotNull(loaded);
        Assert.Equal(150m, loaded.Balance);
        Assert.Equal(3, loaded.Version);
        Assert.Empty(loaded.UncommittedEvents);
        Assert.Equal(3, account.Version);
        Assert.Empty(account.UncommittedEvents);
    }

    [Fact]
    public async Task Second_writer_with_a_stale_version_is_rejected()
    {
        var repository = new AggregateRepository<BankAccount>(new InMemoryEventStore());
        var account = BankAccount.Open(Guid.NewGuid(), "Alice", "EUR");
        account.Deposit(100m);
        await repository.SaveAsync(account, Ct);

        var first = await repository.LoadAsync(account.Id, Ct);
        var second = await repository.LoadAsync(account.Id, Ct);
        first!.Withdraw(80m);
        second!.Withdraw(80m);
        await repository.SaveAsync(first, Ct);

        await Assert.ThrowsAsync<ConcurrencyException>(() => repository.SaveAsync(second, Ct));
        Assert.Equal(20m, (await repository.LoadAsync(account.Id, Ct))!.Balance);
    }

    [Fact]
    public async Task Loading_an_unknown_account_returns_null()
    {
        var repository = new AggregateRepository<BankAccount>(new InMemoryEventStore());

        Assert.Null(await repository.LoadAsync(Guid.NewGuid(), Ct));
    }
}
