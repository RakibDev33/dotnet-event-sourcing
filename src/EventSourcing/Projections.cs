namespace EventSourcing;

/// <summary>A read model built by folding events in global order.</summary>
public interface IProjection
{
    ValueTask ApplyAsync(EventEnvelope envelope, CancellationToken cancellationToken);
}

/// <summary>
/// Catch-up subscription that feeds every event recorded after the current checkpoint to a projection.
/// Persisting <see cref="Checkpoint"/> alongside the read model lets a restarted process resume where it
/// stopped instead of replaying the whole log.
/// </summary>
public sealed class ProjectionRunner(IEventStore store, IProjection projection, long checkpoint = 0)
{
    /// <summary>Global position of the last event applied to the projection.</summary>
    public long Checkpoint { get; private set; } = checkpoint;

    /// <summary>Applies all events newer than the checkpoint and returns how many were applied.</summary>
    public async Task<int> CatchUpAsync(CancellationToken cancellationToken = default)
    {
        var applied = 0;
        await foreach (var envelope in store.ReadAllAsync(Checkpoint, cancellationToken).ConfigureAwait(false))
        {
            await projection.ApplyAsync(envelope, cancellationToken).ConfigureAwait(false);
            Checkpoint = envelope.GlobalPosition;
            applied++;
        }

        return applied;
    }
}
