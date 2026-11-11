using System.Runtime.CompilerServices;

namespace EventSourcing;

/// <summary>
/// Thread-safe in-memory <see cref="IEventStore"/> for tests and prototypes. A durable implementation
/// (SQL, EventStoreDB, Cosmos DB, ...) must honour the same contract: per-stream versions, a dense global
/// position, and atomic appends guarded by the expected version.
/// </summary>
public sealed class InMemoryEventStore(TimeProvider? timeProvider = null) : IEventStore
{
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;
    private readonly Lock _gate = new();
    private readonly List<EventEnvelope> _log = [];
    private readonly Dictionary<Guid, List<EventEnvelope>> _streams = [];

    public Task<IReadOnlyList<EventEnvelope>> ReadStreamAsync(Guid streamId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            IReadOnlyList<EventEnvelope> events = _streams.TryGetValue(streamId, out var stream) ? [.. stream] : [];
            return Task.FromResult(events);
        }
    }

    public Task<long> AppendAsync(
        Guid streamId,
        long expectedVersion,
        IReadOnlyCollection<IDomainEvent> events,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(events);
        if (events.Count == 0)
        {
            throw new ArgumentException("At least one event is required.", nameof(events));
        }

        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            _streams.TryGetValue(streamId, out var stream);
            var currentVersion = stream?.Count ?? 0;
            if (currentVersion != expectedVersion)
            {
                throw new ConcurrencyException(streamId, expectedVersion, currentVersion);
            }

            if (stream is null)
            {
                stream = [];
                _streams.Add(streamId, stream);
            }

            var recordedAt = _time.GetUtcNow();
            foreach (var @event in events)
            {
                var envelope = new EventEnvelope(streamId, stream.Count + 1, _log.Count + 1, recordedAt, @event);
                stream.Add(envelope);
                _log.Add(envelope);
            }

            return Task.FromResult((long)stream.Count);
        }
    }

    public async IAsyncEnumerable<EventEnvelope> ReadAllAsync(
        long afterPosition = 0,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(afterPosition);

        EventEnvelope[] snapshot;
        lock (_gate)
        {
            // Global positions are dense and 1-based, so position p lives at index p - 1.
            snapshot = afterPosition >= _log.Count ? [] : [.. _log.Skip((int)afterPosition)];
        }

        foreach (var envelope in snapshot)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return envelope;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }
}
