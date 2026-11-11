namespace EventSourcing;

/// <summary>Marker interface for events raised by aggregates. Events are immutable facts.</summary>
public interface IDomainEvent
{
}

/// <summary>A stored event: the payload plus its position in its stream and in the global log.</summary>
public sealed record EventEnvelope(
    Guid StreamId,
    long StreamVersion,
    long GlobalPosition,
    DateTimeOffset RecordedAt,
    IDomainEvent Event);

/// <summary>Append-only event storage with per-stream optimistic concurrency.</summary>
public interface IEventStore
{
    /// <summary>Reads all events of a stream in order. Unknown streams return an empty list.</summary>
    Task<IReadOnlyList<EventEnvelope>> ReadStreamAsync(Guid streamId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically appends <paramref name="events"/> if the stream is still at
    /// <paramref name="expectedVersion"/> (0 for a new stream) and returns the new stream version.
    /// </summary>
    /// <exception cref="ConcurrencyException">Another writer appended to the stream first.</exception>
    Task<long> AppendAsync(
        Guid streamId,
        long expectedVersion,
        IReadOnlyCollection<IDomainEvent> events,
        CancellationToken cancellationToken = default);

    /// <summary>Reads events from every stream whose global position is greater than <paramref name="afterPosition"/>.</summary>
    IAsyncEnumerable<EventEnvelope> ReadAllAsync(long afterPosition = 0, CancellationToken cancellationToken = default);
}

/// <summary>Thrown when an append is attempted against a stream version that is no longer current.</summary>
public sealed class ConcurrencyException(Guid streamId, long expectedVersion, long actualVersion)
    : Exception($"Stream {streamId} is at version {actualVersion}, but version {expectedVersion} was expected.")
{
    public Guid StreamId { get; } = streamId;
    public long ExpectedVersion { get; } = expectedVersion;
    public long ActualVersion { get; } = actualVersion;
}
