package com.ctriposs.sdb.benchmark;

import static com.ctriposs.sdb.benchmark.DbBenchmark.DBState.EXISTING;
import static com.ctriposs.sdb.benchmark.DbBenchmark.DBState.FRESH;
import static com.ctriposs.sdb.benchmark.DbBenchmark.Order.RANDOM;
import static com.ctriposs.sdb.benchmark.DbBenchmark.Order.SEQUENTIAL;
import static com.google.common.base.Charsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.xerial.snappy.Snappy;

import com.ctriposs.sdb.utils.Slice;
import com.ctriposs.sdb.utils.SliceOutput;
import com.ctriposs.sdb.utils.Slices;
import com.ctriposs.sdb.utils.TestUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;

public abstract class DbBenchmark {
	protected boolean useExisting, compressMode;
	protected double compressionRatio;
	protected String dbName;
	protected String version;

	private final int num_, valueSize_;
	private int reads_, done_, next_report_;
	private long bytes_, startTime_;
	private String message_;
	private List<String> benchmarks;
	private Random rand_;
	private RandomGenerator gen_;
	private Map<Flag, Object> flags_;

	public abstract void open();

	public abstract void put(byte[] key, byte[] value);

	public abstract byte[] get(byte[] key);

	public abstract void close();

	public abstract void destroyDb();

	public DbBenchmark(String[] commands) {
		inputFlagCommands(commands);
		Map<Flag, Object> flags = flags();
		num_ = (Integer) flags.get(Flag.num);
		valueSize_ = (Integer) flags.get(Flag.value_size);
		compressionRatio = (Double) flags.get(Flag.compression_ratio);
		rand_ = new Random(301);
		gen_ = new RandomGenerator(compressionRatio);
		benchmarks = (List<String>) flags.get(Flag.benchmarks);
		reads_ = (Integer) (flags.get(Flag.reads) == null ? flags.get(Flag.num)
				: flags.get(Flag.reads));
		useExisting = (Boolean) flags.get(Flag.use_existing_db);
		compressMode = (Boolean)flags.get(Flag.compress_mode);
		bytes_ = 0;

		if (!useExisting) {
			destroyDb();
		}
	}

	public String getBaseDir() {
		return (String) flags().get(Flag.base_dir);
	}

	public boolean getCompressMode(){
		return compressMode;
	}

	private Map<Flag, Object> flags() {
		if (flags_ == null) {
			flags_ = new EnumMap<Flag, Object>(Flag.class);
			for (Flag flag : Flag.values()) {
				flags_.put(flag, flag.getDefaultValue());
			}
		}
		return flags_;
	}

	private void inputFlagCommands(String[] commands) {
		for (String arg : commands) {
			boolean valid = false;
			if (arg.startsWith("--")) {
				try {
					ImmutableList<String> parts = ImmutableList.copyOf(Splitter
							.on("=").limit(2).split(arg.substring(2)));
					if (parts.size() != 2) {

					}
					Flag key = Flag.valueOf(parts.get(0));
					Object value = key.parseValue(parts.get(1));
					flags().put(key, value);
					valid = true;
				} catch (Exception e) {
				}
			}

			if (!valid) {
				System.err.println("Invalid argument " + arg);
				System.exit(1);
			}
		}
	}

	public void run() throws IOException {

		printEnvironment();
		printHeader();
		printWarnings();
		System.out.printf("------------------------------------------------\n");

		open();

		for (String benchmark : benchmarks) {
			start();

			boolean known = true;

			if (benchmark.equals("fillseq")) {
				write(SEQUENTIAL, FRESH, num_, valueSize_, 1);
			} else if (benchmark.equals("fillbatch")) {
				write(SEQUENTIAL, FRESH, num_, valueSize_, 1000);
			} else if (benchmark.equals("fillrandom")) {
				write(RANDOM, FRESH, num_, valueSize_, 1);
			} else if (benchmark.equals("overwrite")) {
				write(RANDOM, EXISTING, num_, valueSize_, 1);
			} else if (benchmark.equals("fillsync")) {
				write(RANDOM, FRESH, num_ / 1000, valueSize_, 1);
			} else if (benchmark.equals("fill100K")) {
				write(RANDOM, FRESH, num_ / 1000, 100 * 1000, 1);
			} else if (benchmark.equals("readrandom")) {
				readRandom();
			} else if (benchmark.equals("readhot")) {
				readHot();
			} else if (benchmark.equals("readrandomsmall")) {
				int n = reads_;
				reads_ /= 1000;
				readRandom();
				reads_ = n;
			} else if (benchmark.equals("snappycomp")) {
				snappyCompress();
			} else if (benchmark.equals("snappyuncomp")) {
				snappyUncompressDirectBuffer();
			} else if (benchmark.equals("unsnap-array")) {
				snappyUncompressArray();
			} else if (benchmark.equals("unsnap-direct")) {
				snappyUncompressDirectBuffer();
			} else {
				known = false;
				System.err.println("Unknown benchmark: " + benchmark);
			}
			if (known) {
				stop(benchmark);
			}
		}
		close();
	}

	private void printHeader() throws IOException {
		int kKeySize = 16;

		System.out.printf("Keys: %d bytes each\n", kKeySize);
		System.out.printf(
				"Values: %d bytes each (%d bytes after compression)\n",
				valueSize_, (int) (valueSize_ * compressionRatio + 0.5));
        System.out.printf("Compression: %s\n", compressMode ? "on" : "off");
		System.out.printf("Entries: %d\n", num_);
		System.out.printf("RawSize: %.1f MB (estimated)\n",
				((kKeySize + valueSize_) * num_) / 1048576.0);
		System.out
				.printf("FileSize: %.1f MB (estimated)\n",
						(((kKeySize + valueSize_ * compressionRatio) * num_) / 1048576.0));
	}

	private void printEnvironment()
			throws IOException {
		System.out.printf(dbName + ": version " + version + "\n");
		System.out.printf("Date: %tc\n", new Date());

		File cpuInfo = new File("/proc/cpuinfo");
		if (cpuInfo.canRead()) {
			int numberOfCpus = 0;
			String cpuType = null;
			String cacheSize = null;
			for (String line : CharStreams.readLines(Files.newReader(cpuInfo,
					UTF_8))) {
				ImmutableList<String> parts = ImmutableList.copyOf(Splitter
						.on(':').omitEmptyStrings().trimResults().limit(2)
						.split(line));
				if (parts.size() != 2) {
					continue;
				}
				String key = parts.get(0);
				String value = parts.get(1);

				if (key.equals("model name")) {
					numberOfCpus++;
					cpuType = value;
				} else if (key.equals("cache size")) {
					cacheSize = value;
				}
			}
			System.out.printf("CPU: %d * %s\n", numberOfCpus, cpuType);
			System.out.printf("CPUCache: %s\n", cacheSize);
		}
	}

	private void printWarnings() {
		boolean assertsEnabled = false;
		assert assertsEnabled = true; // Intentional side effect!!!
		if (assertsEnabled) {
			System.out
					.printf("WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
		}

		// See if snappy is working by attempting to compress a compressible
		// string
		String text = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
		byte[] compressedText = null;
		try {
			compressedText = Snappy.compress(text);
		} catch (Exception ignored) {
		}
		if (compressedText == null) {
			System.out.printf("WARNING: Snappy compression is not enabled\n");
		} else if (compressedText.length > text.length()) {
			System.out.printf("WARNING: Snappy compression is not effective\n");
		}
	}

	public void start() {
		startTime_ = System.nanoTime();
		bytes_ = 0;
		message_ = null;
		// hist.clear();
		done_ = 0;
		next_report_ = 100;
	}

	private void write(Order order, DBState state, int numEntries,
			int valueSize, int entries_per_batch) throws IOException {
		if (state == DBState.FRESH) {
			if (useExisting) {
				message_ = "skipping (--use_existing_db is true)";
				return;
			}
			destroyDb();
			open();
			start(); // Do not count time taken to destroy/open
		}

		if (numEntries != num_) {
			message_ = String.format("(%d ops)", numEntries);
		}

		for (int i = 0; i < numEntries; i += entries_per_batch) {
			List<Entry<byte[], byte[]>> batch = new ArrayList<Entry<byte[], byte[]>>();
			for (int j = 0; j < entries_per_batch; j++) {
				int k = (order == Order.SEQUENTIAL) ? i + j : rand_
						.nextInt(num_);
				byte[] key = formatNumber(k);
				batch.add(Maps.immutableEntry(key, gen_.generate(valueSize)));
				bytes_ += valueSize + key.length;
				finishedSingleOp();
			}
			for (Entry<byte[], byte[]> entry : batch) {
				put(entry.getKey(), entry.getValue());
			}
		}
	}

	public void stop(String benchmark) {
		long endTime = System.nanoTime();
		double elapsedSeconds = 1.0d * (endTime - startTime_)
				/ TimeUnit.SECONDS.toNanos(1);

		// Pretend at least one op was done in case we are running a benchmark
		// that does nto call FinishedSingleOp().
		if (done_ < 1) {
			done_ = 1;
		}

		if (bytes_ > 0) {
			String rate = String.format("%6.1f MB/s", (bytes_ / 1048576.0)
					/ elapsedSeconds);
			if (message_ != null) {
				message_ = rate + " " + message_;
			} else {
				message_ = rate;
			}
		} else if (message_ == null) {
			message_ = "";
		}
		System.out.printf("%-12s : %11.5f micros/op;%s%s\n", benchmark,
				elapsedSeconds * 1e6 / done_, (message_ == null ? "" : " "),
				message_);
	}

	private void readRandom() {
		for (int i = 0; i < reads_; i++) {
			byte[] key = formatNumber(rand_.nextInt(num_));
			byte[] value = get(key);
			Preconditions.checkNotNull(value, "db.get(%s) is null", new String(
					key, UTF_8));
			bytes_ += key.length + value.length;
			finishedSingleOp();
		}
	}

	private void readHot() {
		int range = (num_ + 99) / 100;
		for (int i = 0; i < reads_; i++) {
			byte[] key = formatNumber(rand_.nextInt(range));
			byte[] value = get(key);
			bytes_ += key.length + value.length;
			finishedSingleOp();
		}
	}

	private void finishedSingleOp() {
		// if (histogram) {
		// todo
		// }
		done_++;
		if (done_ >= next_report_) {
			if (next_report_ < 1000) {
				next_report_ += 100;
			} else if (next_report_ < 5000) {
				next_report_ += 500;
			} else if (next_report_ < 10000) {
				next_report_ += 1000;
			} else if (next_report_ < 50000) {
				next_report_ += 5000;
			} else if (next_report_ < 100000) {
				next_report_ += 10000;
			} else if (next_report_ < 500000) {
				next_report_ += 50000;
			} else {
				next_report_ += 100000;
			}
			// System.out.printf("... finished %d ops%30s\r", done_, "");

		}
	}

	private static byte[] formatNumber(long n) {
		Preconditions.checkArgument(n >= 0, "number must be positive");

		byte[] slice = new byte[16];

		int i = 15;
		while (n > 0) {
			slice[i--] = (byte) ('0' + (n % 10));
			n /= 10;
		}
		while (i >= 0) {
			slice[i--] = '0';
		}
		return slice;
	}

	public enum Order {
		SEQUENTIAL, RANDOM
	}

	public enum DBState {
		FRESH, EXISTING
	}

	public enum Flag {
		// Comma-separated list of operations to run in the specified order
		// Actual benchmarks:
		// fillseq -- write N values in sequential key order in async mode
		// fillrandom -- write N values in random key order in async mode
		// overwrite -- overwrite N values in random key order in async mode
		// fillsync -- write N/100 values in random key order in sync mode
		// fill100K -- write N/1000 100K values in random order in async mode
		// readrandom -- read N times in random order
		// readhot -- read N times in random order from 1% section of DB
		benchmarks(ImmutableList.<String> of("fillseq", "fillseq", "fillrandom",
				"fillrandom", "fillseq", "readrandom",
				"readrandom", // Extra run to allow previous compactions to quiesce
				"readrandom", "fill100K", "fill100K")) {
			@Override
			public Object parseValue(String value) {
				return ImmutableList.copyOf(Splitter.on(",").trimResults()
						.omitEmptyStrings().split(value));
			}
		},

		// Arrange to generate values that shrink to this fraction of
		// their original size after compression
		compression_ratio(0.5d) {
			@Override
			public Object parseValue(String value) {
				return Double.parseDouble(value);
			}
		},

		// If true, do not destroy the existing database. If you set this
		// flag and also specify a benchmark that wants a fresh database, that
		// benchmark will fail.
		use_existing_db(false) {
			@Override
			public Object parseValue(String value) {
				return Boolean.parseBoolean(value);
			}
		},

		// Number of key/values to place in database
		num(1000000) {
			@Override
			public Object parseValue(String value) {
				return Integer.parseInt(value);
			}
		},

		// Number of read operations to do. If negative, do FLAGS_num reads.
		reads(null) {
			@Override
			public Object parseValue(String value) {
				return Integer.parseInt(value);
			}
		},

		// Size of each value
		value_size(100) {
			@Override
			public Object parseValue(String value) {
				return Integer.parseInt(value);
			}
		},

		base_dir(TestUtil.TEST_BASE_DIR) {
			@Override
			public Object parseValue(String value) {
				return value;
			}
		},

		compress_mode(true) {
			@Override
			public Object parseValue(String value) {
				return Boolean.parseBoolean(value);
			}
		};

		private final Object defaultValue;

		private Flag(Object defaultValue) {
			this.defaultValue = defaultValue;
		}

		public abstract Object parseValue(String value);

		public Object getDefaultValue() {
			return defaultValue;
		}
	}

	public static class RandomGenerator {
		private final Slice data;
		private int position;

		public RandomGenerator(double compressionRatio) {
			// We use a limited amount of data over and over again and ensure
			// that it is larger than the compression window (32KB), and also
			// large enough to serve all typical value sizes we want to write.
			Random rnd = new Random(301);
			data = Slices.allocate(1048576 + 100);
			SliceOutput sliceOutput = data.output();
			while (sliceOutput.size() < 1048576) {
				// Add a short fragment that is as compressible as specified
				// by FLAGS_compression_ratio.
				sliceOutput.writeBytes(compressibleString(rnd,
						compressionRatio, 100));
			}
		}

		protected byte[] generate(int length) {
			if (position + length > data.length()) {
				position = 0;
				assert (length < data.length());
			}
			Slice slice = data.slice(position, length);
			position += length;
			return slice.getBytes();
		}
	}

	private static Slice compressibleString(Random rnd,
			double compressionRatio, int len) {
		int raw = (int) (len * compressionRatio);
		if (raw < 1) {
			raw = 1;
		}
		Slice rawData = generateRandomSlice(rnd, raw);

		// Duplicate the random data until we have filled "len" bytes
		Slice dst = Slices.allocate(len);
		SliceOutput sliceOutput = dst.output();
		while (sliceOutput.size() < len) {
			sliceOutput.writeBytes(rawData, 0,
					Math.min(rawData.length(), sliceOutput.writableBytes()));
		}
		return dst;
	}

	private static Slice generateRandomSlice(Random random, int length) {
		Slice rawData = Slices.allocate(length);
		SliceOutput sliceOutput = rawData.output();
		while (sliceOutput.isWritable()) {
			sliceOutput.writeByte((byte) (' ' + random.nextInt(95)));
		}
		return rawData;
	}

	public void snappyCompress() {
		byte[] raw = gen_.generate(4 * 1024);
		byte[] compressedOutput = new byte[Snappy
				.maxCompressedLength(raw.length)];

		long produced = 0;

		// attempt to compress the block
		while (bytes_ < 1024 * 1048576) { // Compress 1G
			try {
				int compressedSize = Snappy.compress(raw, 0, raw.length,
						compressedOutput, 0);
				bytes_ += raw.length;
				produced += compressedSize;
			} catch (IOException ignored) {
				throw Throwables.propagate(ignored);
			}

			finishedSingleOp();
		}

		message_ = String.format("(output: %.1f%%)", (produced * 100.0)
				/ bytes_);
	}

	public void snappyUncompressArray() {
		int inputSize = 4 * 1024;
		byte[] compressedOutput = new byte[Snappy
				.maxCompressedLength(inputSize)];
		byte raw[] = gen_.generate(inputSize);
		int compressedLength;
		try {
			compressedLength = Snappy.compress(raw, 0, raw.length,
					compressedOutput, 0);
		} catch (IOException e) {
			throw Throwables.propagate(e);
		}
		// attempt to uncompress the block
		while (bytes_ < 5L * 1024 * 1048576) { // Compress 1G
			try {
				Snappy.uncompress(compressedOutput, 0, compressedLength, raw, 0);
				bytes_ += inputSize;
			} catch (IOException ignored) {
				throw Throwables.propagate(ignored);
			}

			finishedSingleOp();
		}
	}

	public void snappyUncompressDirectBuffer() {
		int inputSize = 4 * 1024;
		byte[] compressedOutput = new byte[Snappy
				.maxCompressedLength(inputSize)];
		byte raw[] = gen_.generate(inputSize);
		int compressedLength;
		try {
			compressedLength = Snappy.compress(raw, 0, raw.length,
					compressedOutput, 0);
		} catch (IOException e) {
			throw Throwables.propagate(e);
		}

		ByteBuffer uncompressedBuffer = ByteBuffer.allocateDirect(inputSize);
		ByteBuffer compressedBuffer = ByteBuffer
				.allocateDirect(compressedLength);
		compressedBuffer.put(compressedOutput, 0, compressedLength);

		// attempt to uncompress the block
		while (bytes_ < 5L * 1024 * 1048576) { // Compress 1G
			try {
				uncompressedBuffer.clear();
				compressedBuffer.position(0);
				compressedBuffer.limit(compressedLength);
				Snappy.uncompress(compressedBuffer, uncompressedBuffer);
				bytes_ += inputSize;
			} catch (IOException ignored) {
				throw Throwables.propagate(ignored);
			}

			finishedSingleOp();
		}
	}
}
