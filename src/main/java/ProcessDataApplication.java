import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import okio.ByteString;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

//          funda_utrecht.arrow data :
//            ("id", pa.int32()),
//            ("latitude", pa.float32()),
//            ("longitude", pa.float32()),
//            ("floor_area", pa.int16()),
//            ("plot_area", pa.int32()),
//            ("city", pa.string()),
//            ("neighbourhood", pa.string()),
//            ("postal_code", pa.string()),
//            ("street_name", pa.string()),
//            ("house_number", pa.string()),
//            ("selling_price", pa.int32()),
//            ("energy_label", pa.string()),
//            ("construction_period", pa.string()),
//            ("number_of_rooms", pa.int8()),
//            ("number_of_bedrooms", pa.int8()),

public class ProcessDataApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessDataApplication.class);
    /**
     * Main method: reading batches, filtering and aggregating.
     *
     * @throws IOException If reading from Arrow file fails
     */
    private void doAnalytics() throws IOException {
        RootAllocator allocator = new RootAllocator();

        try (FileInputStream fd = new FileInputStream("funda_utrecht.arrow")) {

            // Setup file reader
            ArrowFileReader fileReader = new ArrowFileReader(new SeekableReadChannel(fd.getChannel()), allocator);
            fileReader.initialize();
            VectorSchemaRoot schemaRoot = fileReader.getVectorSchemaRoot();

            // Aggregate: Using ByteString as it is faster than creating a String from a byte[]
            Map<ByteString, Long> perNeighborhoodCount = new HashMap<>();
            Map<ByteString, Long> perNeighborhoodSum = new HashMap<>();
            processBatches(fileReader, schemaRoot, perNeighborhoodCount, perNeighborhoodSum);

            // Calculate selling price per floor area per neighbourhood
            Map<ByteString, Double> sellingPricePerFloorArea = new TreeMap<>();
            for (ByteString neighborhood : perNeighborhoodCount.keySet()) {
                double average = (double) perNeighborhoodSum.get(neighborhood) / perNeighborhoodCount.get(neighborhood);
                sellingPricePerFloorArea.put(neighborhood, average);
            }

            // Sort the neighborhoods by average selling price (highest to lowest)
            List<Map.Entry<ByteString, Double>> sortedNeighborhoods = new ArrayList<>(sellingPricePerFloorArea.entrySet());
            sortedNeighborhoods.sort(new SellingPriceComparator());

            // Print results (sorted)
            for (Map.Entry<ByteString, Double> entry : sortedNeighborhoods) {
                ByteString neighborhood = entry.getKey();
                double average = entry.getValue();
                LOGGER.info("Neighborhood = {}; Average = {}", neighborhood, average);
            }
        }
    }

//    A comparator to sort the neighbourhoods on average price
    private static class SellingPriceComparator implements Comparator<Map.Entry<ByteString, Double>> {
        @Override
        public int compare(Map.Entry<ByteString, Double> entry1, Map.Entry<ByteString, Double> entry2) {
            return Double.compare(entry2.getValue(), entry1.getValue()); // Compare in reverse order (highest to lowest)
        }
    }
    /**
     * Read batches, apply filters and write aggregation values into aggregation data structures
     *
     * @param fileReader   Reads batches from Arrow file
     * @param schemaRoot   Schema root for read batches
     * @param perNeighborhoodCount Aggregation of count per city
     * @param perNeighborhoodSum   Aggregation of summed value per city
     * @throws IOException If reading the arrow file goes wrong
     */
    private void processBatches(ArrowFileReader fileReader,
                                VectorSchemaRoot schemaRoot,
                                Map<ByteString, Long> perNeighborhoodCount,
                                Map<ByteString, Long> perNeighborhoodSum) throws IOException {
        // Reading the data, one batch at a time
        while (fileReader.loadNextBatch()) {
            IntArrayList neighborhoodSelectedIndexes = filterOnNeighbourhood(schemaRoot);
            IntArrayList sellingPriceSelectedIndexes = filterOnSellingPrice(schemaRoot);
            IntArrayList streetSelectedIndexes = filterOnStreet(schemaRoot);
            IntArrayList perNeighbourhoodSellingPriceIndexes = PricePerFloorArea(fileReader,schemaRoot);

            IntArrayList neighbourhoodAndSellingPrice = intersection(neighborhoodSelectedIndexes, sellingPriceSelectedIndexes);
            IntArrayList allIntersected = intersection(neighbourhoodAndSellingPrice, streetSelectedIndexes);
            int[] selectedIndexes = allIntersected.elements();

            aggregate(schemaRoot, selectedIndexes, perNeighborhoodCount, perNeighborhoodSum);
        }
    }

    /**
     * Given the selected indexes, it copies the aggregation values into aggregation vectors
     *
     * @param schemaRoot      Schema root of batch
     * @param selectedIndexes Indexes to aggregate
     * @param perNeighbourhoodCount    Aggregating counts per city
     * @param perNeighbourhoodSum      Aggregating sums per city
     */
    private void aggregate(VectorSchemaRoot schemaRoot,
                           int[] selectedIndexes,
                           Map<ByteString, Long> perNeighbourhoodCount,
                           Map<ByteString, Long> perNeighbourhoodSum) {
        VarCharVector neighbourhoodVector = (VarCharVector) schemaRoot.getVector("neighbourhood");
        IntVector sellingPriceDataVector = (IntVector) schemaRoot.getVector("selling_price");

        for (int selectedIndex : selectedIndexes) {
            if (!sellingPriceDataVector.isNull(selectedIndex)){
                ByteString neighbourhood = ByteString.of(neighbourhoodVector.get(selectedIndex));
                perNeighbourhoodCount.put(neighbourhood, perNeighbourhoodCount.getOrDefault(neighbourhood, 0L) + 1);
                perNeighbourhoodSum.put(neighbourhood, perNeighbourhoodSum.getOrDefault(neighbourhood, 0L) + sellingPriceDataVector.get(selectedIndex));
            }
        }
    }
    //     Calculate selling price per floor area per neighbourhood
    private IntArrayList PricePerFloorArea(ArrowFileReader fileReader, VectorSchemaRoot schemaRoot) throws IOException {
        Map<ByteString, Long> perNeighbourhoodSellingPrice = new HashMap<>();
        Map<ByteString, Long> perNeighbourhoodFloorArea = new HashMap<>();

        // Process the data and calculate selling price and floor area per neighbourhood
        IntArrayList selectedIndexes = new IntArrayList();
        VarCharVector neighbourhoodVector = (VarCharVector) schemaRoot.getVector("neighbourhood");
        IntVector sellingPriceDataVector = (IntVector) schemaRoot.getVector("selling_price");
        SmallIntVector floorAreaVector = (SmallIntVector) schemaRoot.getVector("floor_area");

        for (int i = 0; i < schemaRoot.getRowCount(); i++) {

            if (!sellingPriceDataVector.isNull(i) && !floorAreaVector.isNull(i)) {
                ByteString neighbourhood = ByteString.of(neighbourhoodVector.get(i));
                long sellingPrice = sellingPriceDataVector.get(i);
                long floorArea = floorAreaVector.get(i);

                perNeighbourhoodSellingPrice.put(neighbourhood, perNeighbourhoodSellingPrice.getOrDefault(neighbourhood, 0L) + sellingPrice);
                perNeighbourhoodFloorArea.put(neighbourhood, perNeighbourhoodFloorArea.getOrDefault(neighbourhood, 0L) + floorArea);
                selectedIndexes.add(i);

                // Keep track of selected indexes for further processing
            }
        }

        // Print results
        for (ByteString neighbourhood : perNeighbourhoodSellingPrice.keySet()) {
            long sellingPrice = perNeighbourhoodSellingPrice.get(neighbourhood);
            long floorArea = perNeighbourhoodFloorArea.get(neighbourhood);
            double pricePerArea = (double) sellingPrice / floorArea;
            LOGGER.info("Neighbourhood = {}; Price per Floor Area = {}", neighbourhood, pricePerArea);
        }

        return selectedIndexes;
    }

    //     Search neighbourhoods based on their names with the "".getBytes
    private IntArrayList filterOnNeighbourhood(VectorSchemaRoot schemaRoot) {
        VarCharVector neighbourhood = (VarCharVector) schemaRoot.getVector("neighbourhood");
        IntArrayList neighbourhoodSelectedIndexes = new IntArrayList();
//        byte[] prefixBytes = "".getBytes();
        for (int i = 0; i < schemaRoot.getRowCount(); i++) {
//            if (ByteString.of(neighbourhood.get(i)).startsWith(prefixBytes)) {
            neighbourhoodSelectedIndexes.add(i);
//            }
        }
        neighbourhoodSelectedIndexes.trim();
        return neighbourhoodSelectedIndexes;
    }

//    filter based on price
    private IntArrayList filterOnSellingPrice(VectorSchemaRoot schemaRoot) {
        IntVector sellingPrice = (IntVector) schemaRoot.getVector("selling_price");
        IntArrayList sellingPriceSelectedIndexes = new IntArrayList();
        for (int i = 0; i < schemaRoot.getRowCount(); i++) {
//            int currentSellingPrice = sellingPrice.get(i);
//            if (0 <= currentSellingPrice && currentSellingPrice <= 10000000) {
            sellingPriceSelectedIndexes.add(i);
//            }
        }
        sellingPriceSelectedIndexes.trim();
        return sellingPriceSelectedIndexes;
    }

    // Filter based on street names
    private IntArrayList filterOnStreet(VectorSchemaRoot schemaRoot) {
        VarCharVector streetVector = (VarCharVector) schemaRoot.getVector("street_name");

        IntArrayList streetSelectedIndexes = new IntArrayList();
//        byte[] suffixBytes = "".getBytes();
        for (int i = 0; i < schemaRoot.getRowCount(); i++) {
//            if (ByteString.of(streetVector.get(i)).endsWith(suffixBytes)) {
                streetSelectedIndexes.add(i);
//            }
        }
        streetSelectedIndexes.trim();
        return streetSelectedIndexes;
    }

    /**
     * Outputs intersection of two sorted integer arrays
     *
     * @param x first array
     * @param y second array
     * @return Intersection of x and y
     */
    private IntArrayList intersection(IntList x, IntList y) {
        int indexX = 0;
        int indexY = 0;
        IntArrayList intersection = new IntArrayList();

        while (indexX < x.size() && indexY < y.size()) {
            if (x.getInt(indexX) < y.getInt(indexY)) {
                indexX++;
            } else if (x.getInt(indexX) > y.getInt(indexY)) {
                indexY++;
            } else {
                intersection.add(x.getInt(indexX));
                indexX++;
                indexY++;
            }
        }

        intersection.trim();
        return intersection;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("io.netty.tryReflectionSetAccessible","true");
        ProcessDataApplication app = new ProcessDataApplication();
        app.doAnalytics();
//        StopWatch stopWatch = StopWatch.createStarted();
//        stopWatch.stop();
//        LOGGER.info("Timing: {}", stopWatch);
    }
}
