namespace EventSourcing.Tests;

public sealed class InMemoryEventStoreTests
{
    private sealed record Happened(int Number) : IDomainEvent;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Append_assigns_stream_versions_and_dense_global_positions()
    {
        var store = new InMemoryEventStore();
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();

        await store.AppendAsync(first, 0, [new Happened(1), new Happened(2)], Ct);
        await store.AppendAsync(second, 0, [new Happened(3)], Ct);
        var newVersion = await store.AppendAsync(first, 2, [new Happened(4)], Ct);

        Assert.Equal(3, newVersion);
        var stream = await store.ReadStreamAsync(first, Ct);
        Assert.Equal(3, stream.Count);
        Assert.Equal(3, stream[2].StreamVersion);
        Assert.Equal(4, stream[2].GlobalPosition);
        Assert.Equal(new Happened(4), stream[2].Event);
    }

    [Fact]
    public async Task Append_with_a_stale_expected_version_throws_and_writes_nothing()
    {
        var store = new InMemoryEventStore();
        var id = Guid.NewGuid();
        await store.AppendAsync(id, 0, [new Happened(1)], Ct);

        var conflict = await Assert.ThrowsAsync<ConcurrencyException>(
            () => store.AppendAsync(id, 0, [new Happened(2)], Ct));

        Assert.Equal(0, conflict.ExpectedVersion);
        Assert.Equal(1, conflict.ActualVersion);
        Assert.Single(await store.ReadStreamAsync(id, Ct));
    }

    [Fact]
    public async Task Reading_an_unknown_stream_returns_no_events()
    {
        var store = new InMemoryEventStore();

        Assert.Empty(await store.ReadStreamAsync(Guid.NewGuid(), Ct));
    }

    [Fact]
    public async Task ReadAll_returns_only_events_after_the_given_position_in_global_order()
    {
        var store = new InMemoryEventStore();
        await store.AppendAsync(Guid.NewGuid(), 0, [new Happened(1), new Happened(2)], Ct);
        await store.AppendAsync(Guid.NewGuid(), 0, [new Happened(3)], Ct);

        var positions = new List<long>();
        await foreach (var envelope in store.ReadAllAsync(afterPosition: 1, Ct))
        {
            positions.Add(envelope.GlobalPosition);
        }

        Assert.Equal(2, positions.Count);
        Assert.Equal(2, positions[0]);
        Assert.Equal(3, positions[1]);
    }

    [Fact]
    public async Task Concurrent_appends_to_different_streams_all_get_unique_positions()
    {
        var store = new InMemoryEventStore();

        await Task.WhenAll(Enumerable.Range(0, 100).Select(i =>
            Task.Run(() => store.AppendAsync(Guid.NewGuid(), 0, [new Happened(i)], Ct), Ct)));

        var positions = new HashSet<long>();
        await foreach (var envelope in store.ReadAllAsync(cancellationToken: Ct))
        {
            positions.Add(envelope.GlobalPosition);
        }

        Assert.Equal(100, positions.Count);
        Assert.Equal(100, positions.Max());
    }

    [Fact]
    public async Task Appending_an_empty_batch_is_rejected()
    {
        var store = new InMemoryEventStore();

        await Assert.ThrowsAsync<ArgumentException>(() => store.AppendAsync(Guid.NewGuid(), 0, [], Ct));
    }
}
