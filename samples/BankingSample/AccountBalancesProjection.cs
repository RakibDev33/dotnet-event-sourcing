using EventSourcing;

namespace BankingSample;

public sealed record AccountSummary(
    Guid AccountId,
    string Owner,
    string Currency,
    decimal Balance,
    int TransactionCount,
    bool IsClosed);

/// <summary>Read model with one summary row per account, kept up to date by a <see cref="ProjectionRunner"/>.</summary>
public sealed class AccountBalancesProjection : IProjection
{
    private readonly Dictionary<Guid, AccountSummary> _accounts = [];

    public IReadOnlyCollection<AccountSummary> Accounts => _accounts.Values;

    public AccountSummary? Find(Guid accountId) => _accounts.GetValueOrDefault(accountId);

    public ValueTask ApplyAsync(EventEnvelope envelope, CancellationToken cancellationToken)
    {
        var id = envelope.StreamId;
        switch (envelope.Event)
        {
            case AccountOpened opened:
                _accounts[id] = new AccountSummary(id, opened.Owner, opened.Currency, 0m, 0, IsClosed: false);
                break;
            case MoneyDeposited deposited:
                Update(id, summary => summary with
                {
                    Balance = summary.Balance + deposited.Amount,
                    TransactionCount = summary.TransactionCount + 1,
                });
                break;
            case MoneyWithdrawn withdrawn:
                Update(id, summary => summary with
                {
                    Balance = summary.Balance - withdrawn.Amount,
                    TransactionCount = summary.TransactionCount + 1,
                });
                break;
            case AccountClosed:
                Update(id, summary => summary with { IsClosed = true });
                break;
        }

        return ValueTask.CompletedTask;
    }

    private void Update(Guid id, Func<AccountSummary, AccountSummary> change)
    {
        if (_accounts.TryGetValue(id, out var current))
        {
            _accounts[id] = change(current);
        }
    }
}
