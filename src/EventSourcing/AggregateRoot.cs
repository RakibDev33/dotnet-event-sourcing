namespace EventSourcing;

/// <summary>
/// Base class for event-sourced aggregates.
/// </summary>
/// <remarks>
/// Behaviour methods validate their input and then call <see cref="Raise"/>; state only ever changes in
/// <see cref="When"/>. Rehydration replays the same <see cref="When"/> method, so state built live and
/// state rebuilt from history cannot drift apart.
/// </remarks>
public abstract class AggregateRoot
{
    private readonly List<IDomainEvent> _uncommittedEvents = [];

    /// <summary>Stream identifier. Must be set by the first event the aggregate applies.</summary>
    public Guid Id { get; protected set; }

    /// <summary>Number of events already persisted for this aggregate; 0 for a new one.</summary>
    public long Version { get; private set; }

    /// <summary>Events raised since the aggregate was loaded or last saved.</summary>
    public IReadOnlyList<IDomainEvent> UncommittedEvents => _uncommittedEvents;

    /// <summary>Replays persisted events to rebuild state. Used by repositories.</summary>
    public void LoadFromHistory(IEnumerable<IDomainEvent> history)
    {
        ArgumentNullException.ThrowIfNull(history);

        foreach (var @event in history)
        {
            When(@event);
            Version++;
        }
    }

    /// <summary>Marks uncommitted events as persisted. Call only after a successful append.</summary>
    public void MarkCommitted()
    {
        Version += _uncommittedEvents.Count;
        _uncommittedEvents.Clear();
    }

    /// <summary>Applies a new event to the aggregate and records it for persistence.</summary>
    protected void Raise(IDomainEvent @event)
    {
        ArgumentNullException.ThrowIfNull(@event);
        When(@event);
        _uncommittedEvents.Add(@event);
    }

    /// <summary>Mutates state for one event. Must not validate or throw: events are facts that already happened.</summary>
    protected abstract void When(IDomainEvent @event);
}
