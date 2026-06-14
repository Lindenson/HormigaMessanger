package org.hormigas.ws.ports.watermark;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.watremark.Watermark;

import java.util.List;

public interface WatermarksRegistry {
    Uni<Void> add(Watermark watermark);
    Uni<List<Watermark>> fetchBatch(int limit);
}
