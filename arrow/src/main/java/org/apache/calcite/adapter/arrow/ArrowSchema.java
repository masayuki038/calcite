package org.apache.calcite.adapter.arrow;

import com.google.common.base.Function;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.file.ArrowFileReader;
import org.apache.arrow.vector.file.SeekableReadChannel;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.calcite.tools.Program;
import org.apache.calcite.util.Holder;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Schema for Apache Arrow
 */
public class ArrowSchema extends AbstractSchema {

    private Map<String, Table> tableMap;
    private File directory;

    public ArrowSchema(File directory) {
        this.directory = directory;
//        Hook.PROGRAM.add(new Function<Holder<Program>, Void>() {
//            public Void apply(Holder<Program> holder) {
//                holder.set(ArrowPrograms.standard());
//                return null;
//            }
//        });
    }

    private String trim(String s, String suffix) {
        String trimmed = trimOrNull(s, suffix);
        if (trimmed == null) {
            return s;
        }
        return trimmed;
    }

    private String trimOrNull(String s, String suffix) {
        if (s.endsWith(suffix)) {
            return s.substring(0, s.length() - suffix.length());
        }
        return null;
    }

    public Map<String, Table> getTableMap() {
        if (tableMap == null) {
            tableMap = new HashMap<>();
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            File[] arrowFiles = directory.listFiles((dir, name) -> name.endsWith(".arrow"));
            Arrays.stream(arrowFiles).forEach(file -> {
                try {
                    VectorSchemaRoot[] vectorSchemaRoots = load(file.getAbsolutePath(), allocator);
                    tableMap.put(trim(file.getName(), ".arrow").toUpperCase(), new ArrowTable(vectorSchemaRoots, null));
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            });
        }
        return tableMap;
    }

    private VectorSchemaRoot[] load(String path, BufferAllocator allocator) throws IOException {
        byte[] bytes = Files.readAllBytes(FileSystems.getDefault().getPath(path));
        SeekableReadChannel channel = new SeekableReadChannel(new ByteArrayReadableSeekableByteChannel(bytes));
        ArrowFileReader reader = new ArrowFileReader(channel, allocator);
        List<VectorSchemaRoot> list = reader.getRecordBlocks().stream().map(block -> {
            try {
                if (!reader.loadRecordBatch(block)) {
                    throw new IllegalStateException("Failed to load RecordBatch");
                }
                return reader.getVectorSchemaRoot();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }).collect(Collectors.toList());
        VectorSchemaRoot[] vectorSchemaRoots = new VectorSchemaRoot[list.size()];
        list.toArray(vectorSchemaRoots);
        return vectorSchemaRoots;
    }
}
