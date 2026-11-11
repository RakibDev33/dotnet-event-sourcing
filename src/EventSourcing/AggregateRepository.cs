namespace EventSourcing;

/// <summary>Loads aggregates by replaying their stream and saves new events with optimistic concurrency.</summary>
public sealed class AggregateRepository<TAggregate>(IEventStore store)
    where TAggregate : AggregateRoot, new()
{
    /// <summary>Rebuilds the aggregate from its events, or returns <c>null</c> if the stream does not exist.</summary>
    public async Task<TAggregate?> LoadAsync(Guid id, CancellationToken cancellationToken = default)
    {
        var events = await store.ReadStreamAsync(id, cancellationToken).ConfigureAwait(false);
        if (events.Count == 0)
        {
            return null;
        }

        var aggregate = new TAggregate();
        aggregate.LoadFromHistory(events.Select(envelope => envelope.Event));
        return aggregate;
    }

    /// <summary>
    /// Appends the aggregate's uncommitted events, expecting the stream to be at the version the aggregate was loaded at.
    /// </summary>
    /// <exception cref="ConcurrencyException">The stream changed after the aggregate was loaded.</exception>
    public async Task SaveAsync(TAggregate aggregate, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(aggregate);

        if (aggregate.UncommittedEvents.Count == 0)
        {
            return;
        }

        if (aggregate.Id == Guid.Empty)
        {
            throw new InvalidOperationException("The aggregate has no id. Its first event must assign one.");
        }

        await store.AppendAsync(aggregate.Id, aggregate.Version, [.. aggregate.UncommittedEvents], cancellationToken)
            .ConfigureAwait(false);
        aggregate.MarkCommitted();
    }
}
