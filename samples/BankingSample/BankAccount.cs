using EventSourcing;

namespace BankingSample;

public sealed record AccountOpened(Guid AccountId, string Owner, string Currency) : IDomainEvent;

public sealed record MoneyDeposited(decimal Amount) : IDomainEvent;

public sealed record MoneyWithdrawn(decimal Amount) : IDomainEvent;

public sealed record AccountClosed : IDomainEvent;

/// <summary>Event-sourced bank account: no overdrafts, and only empty accounts can be closed.</summary>
public sealed class BankAccount : AggregateRoot
{
    public string Owner { get; private set; } = string.Empty;
    public string Currency { get; private set; } = string.Empty;
    public decimal Balance { get; private set; }
    public bool IsClosed { get; private set; }

    public static BankAccount Open(Guid id, string owner, string currency)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(owner);
        if (id == Guid.Empty)
        {
            throw new ArgumentException("Account id is required.", nameof(id));
        }

        if (currency is not { Length: 3 })
        {
            throw new ArgumentException("Currency must be a three-letter ISO 4217 code.", nameof(currency));
        }

        var account = new BankAccount();
        account.Raise(new AccountOpened(id, owner.Trim(), currency.ToUpperInvariant()));
        return account;
    }

    public void Deposit(decimal amount)
    {
        EnsureOpen();
        EnsurePositive(amount);
        Raise(new MoneyDeposited(amount));
    }

    public void Withdraw(decimal amount)
    {
        EnsureOpen();
        EnsurePositive(amount);
        if (amount > Balance)
        {
            throw new InvalidOperationException(
                $"Insufficient funds: balance is {Balance} {Currency}, requested {amount} {Currency}.");
        }

        Raise(new MoneyWithdrawn(amount));
    }

    public void Close()
    {
        EnsureOpen();
        if (Balance != 0)
        {
            throw new InvalidOperationException("Only accounts with a zero balance can be closed.");
        }

        Raise(new AccountClosed());
    }

    protected override void When(IDomainEvent @event)
    {
        switch (@event)
        {
            case AccountOpened opened:
                Id = opened.AccountId;
                Owner = opened.Owner;
                Currency = opened.Currency;
                break;
            case MoneyDeposited deposited:
                Balance += deposited.Amount;
                break;
            case MoneyWithdrawn withdrawn:
                Balance -= withdrawn.Amount;
                break;
            case AccountClosed:
                IsClosed = true;
                break;
            default:
                throw new InvalidOperationException($"{nameof(BankAccount)} cannot apply {@event.GetType().Name}.");
        }
    }

    private void EnsureOpen()
    {
        if (IsClosed)
        {
            throw new InvalidOperationException("The account is closed.");
        }
    }

    private static void EnsurePositive(decimal amount) => ArgumentOutOfRangeException.ThrowIfNegativeOrZero(amount);
}
