/*
 * jTPCC - Open Source Java implementation of a TPC-C like benchmark
 *
 * Copyright (C) 2003, Raul Barbosa
 * Copyright (C) 2004-2006, Denis Lussier
 *
 */

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.text.*;

public class jTPCC implements jTPCCConfig {

	private static String configInfo = "N/A"; // any extra config info. on the benchmark being run.
	private static String hotStart = "false"; // whether the database is being run 'hot'.
	private static boolean silent = false; // whether to record anything from this execution of the benchmark.

	private jTPCCTerminal[] terminals;
	private String[] terminalNames;
	private Random random;
	private long terminalsStarted = 0, sessionCount = 0, transactionCount;

	private long newOrderCounter, sessionStartTimestamp, sessionEndTimestamp, sessionNextTimestamp = 0, sessionNextKounter = 0;
	private long sessionEndTargetTime = -1, executedTransactionCounter, recentTpmC = 0;
	private boolean signalTerminalsRequestEndSent = false, databaseDriverLoaded = false;

	private String sessionStart, sessionEnd;

	private String database;
	private String driver;
	private String username;
	private String password;
	private int numTerminals;
	private int numWarehouses;
	private boolean debugMessages;
	private int minutes;
	private int transactionsPerTerminal;
	private boolean limitIsTime;
	private int paymentWeight;
	private int orderStatusWeight;
	private int stockLevelWeight;
	private int deliveryWeight;
	private boolean thereWereErrors = false;
	private OutputStream normalOutput;
	private OutputStream errorOutput;
	private OutputStream summaryOutput;

	private boolean terminalsFinished = false;
	private String databaseName;
	private Date startTime;
	private DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
	private final String logFileLocation;
	private final int numberOfReplicas;

	private boolean writeStandardOutToFile;

	//factor
	private float factor;

	/**
	 * Start benchmarksql benchmark.
	 *
	 * @param args
	 *            <ul>
	 *            <li>-w: The number of warehouses to use in the benchmark (default=1)</li>
	 *            <li>-t: The number of terminals to use in the benchmark (default=1)</li>
	 *            <li>-log: Location where benchmark results will be stored (default=current working directory). This option creates a
	 *            folder called 'benchmarkresults' at the specified location.</li>
	 *            <li>-c: Additional configuration info. For example, the name of the database conf file being used can be passed here, and
	 *            it will be added alongside benchmark results.</li>
	 *            <li>-h: Whether this is a 'hot start run'. If it is, the results of this execution are not recorded in any log files.</li>
	 *            <li>-m: Number of minutes to execute the benchmark for (default=1).</li>
	 *            <li>-silent: Use this to disable messages to System.out for every set of rows inserted.</li>
	 *            <li>-stdout: Write standard out to file (takes up Mb of disk space for each run).</li>
	 *            <li>-f: factor less than one, so we can have smaller test.</li>
	 *            </ul>
	 */
	public static void main(String args[]) {
		String logFileLocation = "";

		int numWarehouses = 1;
		int numTerminals = 1;
		int minutesToExecute = 1;
		int numberOfReplicas = -1;
		float factor = 1.0f;
		boolean writeStandardOutToFile = true;

		for (int i = 0; i < args.length; i++) {
			String str = args[i];
			if (str.toLowerCase().startsWith("-w")) { // number of warehouses.
				String val = args[i].substring("-t".length());
				System.out.println("Setting the number of warehouses to: " + val);
				numWarehouses = Integer.parseInt(val);
			}

			else if (str.toLowerCase().startsWith("-t")) { // number of terminals
				String val = args[i].substring("-t".length());
				System.out.println("Setting the number of terminals to: " + val);
				numTerminals = Integer.parseInt(val);
			}

			else if (str.toLowerCase().startsWith("-log")) {
				logFileLocation = args[i].substring("-log".length());
				logFileLocation = logFileLocation
						+ (logFileLocation.length() > 0 && !logFileLocation.endsWith(File.separator) ? File.separator : "");

			} else if (str.toLowerCase().startsWith("-c")) {
				configInfo = args[i].substring("-c".length());
			} else if (str.toLowerCase().startsWith("-h")) {
				hotStart = args[i].substring("-h".length());
			} else if (str.toLowerCase().startsWith("-m")) { // number of minutes to execute transactions for.
				minutesToExecute = Integer.parseInt(args[i].substring("-m".length()));
			} else if (str.toLowerCase().startsWith("-silent")) { // don't record anything from this run.
				silent = true;
			} else if (str.toLowerCase().startsWith("-r")) { // number of minutes to execute transactions for.

				numberOfReplicas = Integer.parseInt(args[i].substring("-r".length()));
			} else if (str.toLowerCase().startsWith("-f")) { // number of minutes to execute transactions for.

				factor = Float.parseFloat(args[i].substring("-f".length()));
				if (factor > 1) {
					factor = 1.0f;
				}
			}else if (str.toLowerCase().startsWith("-stdout")) { // number of minutes to execute transactions for.

				writeStandardOutToFile = true;
		}
		}

		jTPCC benchmark = new jTPCC(logFileLocation, numWarehouses, numTerminals, minutesToExecute, numberOfReplicas, writeStandardOutToFile, factor);

		try {
			benchmark.run();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public jTPCC(String logFileLocation, int numWarehouses, int numTerminals, int minutes, int numberOfReplicas, boolean writeStandardOutToFile,
			float factor) {

		this.factor = factor;
		this.numWarehouses = numWarehouses;
		this.numTerminals = numTerminals;
		this.minutes = minutes;
		this.numberOfReplicas = numberOfReplicas;
		this.writeStandardOutToFile = writeStandardOutToFile;
		this.logFileLocation = logFileLocation
				+ (logFileLocation.length() > 0 && !logFileLocation.endsWith(File.separator) ? File.separator : "");

		// load the ini file
		Properties ini = new Properties();
		try {
			ini.load(new FileInputStream(System.getProperty("prop")));
		} catch (IOException e) {
			System.out.println("could not load properties file");
		}

		// display the values we need

		System.out.println("Config: [" + ini.getProperty("driver") + ", " + ini.getProperty("conn") + ", " + ini.getProperty("user") + "]");

		this.random = new Random(System.currentTimeMillis());

		databaseName = ini.getProperty("name", "Unknown");
		database = ini.getProperty("conn", defaultDatabase);
		driver = ini.getProperty("driver", defaultDriver);
		username = ini.getProperty("user", defaultUsername);
		password = ini.getProperty("password", defaultPassword);

		debugMessages = defaultDebugMessages;
		minutes = defaultMinutes;
		transactionsPerTerminal = defaultTransactionsPerTerminal;
		limitIsTime = defaultRadioTime;

		paymentWeight = defaultPaymentWeight;
		orderStatusWeight = defaultOrderStatusWeight;
		deliveryWeight = defaultDeliveryWeight;
		stockLevelWeight = defaultStockLevelWeight;



	}

	public void run() throws Exception {

		createTraceOutputFolders();

		System.out.println("\tRunning benchmark for " + minutes + " minute" + ((minutes > 1) ? "s" : "")
				+ (silent ? " in hot start mode" : "") + ".");
		createTerminals();
		startTransactions();
		waitUntilTerminalsFinish();

		if (!silent) {
			writeResultToResultsFile(databaseName, minutes * 60, executedTransactionCounter);

			if (thereWereErrors) {
				System.out.println("There were errors during this benchmark run.");
			}

			if (writeStandardOutToFile) normalOutput.flush();
			errorOutput.flush();
			summaryOutput.flush();

		}

		System.out.println("\tBenchmark results" + (silent ? " HOT START" : "") + " [" + databaseName + ", " + minutes * 60 + " seconds]: "
				+ executedTransactionCounter + " executed transactions.");

		if (writeStandardOutToFile) normalOutput.close();
		errorOutput.close();
		summaryOutput.close();
	}

	private void writeResultToResultsFile(String executingDatabaseName, int timeOfTestSeconds, long numberOfExecutedTransactions) {
		String resultsFolderPath = logFileLocation + "benchmark-results" + File.separator + "results-summary";

		// Create folder if it doesn't exist.
		File resultsFolder = new File(resultsFolderPath);
		resultsFolder.mkdirs();

		// Create file if it doesn't exist.
		File specificDbResultsFile = new File(resultsFolderPath + File.separator + executingDatabaseName + ".csv");
		addToResultsFile(executingDatabaseName, timeOfTestSeconds, numberOfExecutedTransactions, specificDbResultsFile);

		File genericResultsFile = new File(resultsFolderPath + File.separator + "all.csv");
		addToResultsFile(executingDatabaseName, timeOfTestSeconds, numberOfExecutedTransactions, genericResultsFile);
	}

	public void addToResultsFile(String executingDatabaseName, int timeOfTestSeconds, long numberOfExecutedTransactions, File resultsFile) {
		boolean resultsFileCreated = false;

		try {
			if (!resultsFile.exists()) {

				resultsFileCreated = resultsFile.createNewFile();

			}

			OutputStream resultsFileStream = new FileOutputStream(resultsFile, true);

			if (resultsFileCreated) {
				// Write header lines.

				String header = "timeOfTest, durationOfTest, databaseBeingTested, numberOfTransactionsExecuted, runHot, config, replicas"
						+ "\n";

				resultsFileStream.write(header.getBytes());
			}

			String results = startTime.getTime() + ", " + timeOfTestSeconds + ", " + executingDatabaseName + ", "
					+ numberOfExecutedTransactions + ", " + hotStart + ", " + configInfo + ", " + numberOfReplicas + "\n";
			resultsFileStream.write(results.getBytes());

			resultsFileStream.flush();

			resultsFileStream.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createTraceOutputFolders() {
		try {

			startTime = new Date();

			String pathToResults = logFileLocation + "benchmark-results" + File.separator + dateFormatter.format(startTime);
			System.out.println("Path to results: " + pathToResults);

			File f = new File(pathToResults);
			f.mkdirs();

			if (writeStandardOutToFile){
				normalOutput = new FileOutputStream(pathToResults + File.separator + "normal.txt");
			}
			errorOutput = new FileOutputStream(pathToResults + File.separator + "error.txt");
			summaryOutput = new FileOutputStream(pathToResults + File.separator + "summary.txt");

		} catch (FileNotFoundException e) {
			System.err
					.println("Log files could not be created, so the benchmark will not run. You can probably fix the path these logs are being written to by using the -log parameter.");
			e.printStackTrace();
			System.exit(1);
		}
	}

	private void waitUntilTerminalsFinish() {
		while (!areAllTerminalsFinished()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void createTerminals() throws Exception {
		removeAllTerminals();
		executedTransactionCounter = 0;

		try {

			printMessage("Loading database driver: \'" + driver + "\'...");
			Class.forName(driver);
			databaseDriverLoaded = true;
		} catch (Exception ex) {
			errorMessage("Unable to load the database driver!");
			databaseDriverLoaded = false;
		}

		if (databaseDriverLoaded) {
			long executionTimeMillis = -1;

			try {

				if (numTerminals <= 0 || numTerminals > 10 * numWarehouses)
					throw new NumberFormatException();
			} catch (NumberFormatException e1) {
				errorMessage("Invalid number of terminals!");
				throw new Exception();
			}

			if (limitIsTime) {
				try {
					executionTimeMillis = minutes * 60000;
					if (executionTimeMillis <= 0)
						throw new NumberFormatException();
				} catch (NumberFormatException e1) {
					errorMessage("Invalid number of minutes!");
					throw new Exception();
				}
			} else {
				try {
					if (transactionsPerTerminal <= 0)
						throw new NumberFormatException();
				} catch (NumberFormatException e1) {
					errorMessage("Invalid number of transactions per terminal!");
					throw new Exception();
				}
			}

			try {

				if (paymentWeight < 0 || orderStatusWeight < 0 || deliveryWeight < 0 || stockLevelWeight < 0)
					throw new NumberFormatException();
			} catch (NumberFormatException e1) {
				errorMessage("Invalid number in mix percentage!");
				throw new Exception();
			}

			if (paymentWeight + orderStatusWeight + deliveryWeight + stockLevelWeight > 100) {
				errorMessage("Sum of mix percentage parameters exceeds 100%!");
				throw new Exception();
			}

			newOrderCounter = 0;
			printMessage("Session #" + (++sessionCount) + " started!");
			if (!limitIsTime)
				printMessage("Creating " + numTerminals + " terminal(s) with " + transactionsPerTerminal
						+ " transaction(s) per terminal...");
			else
				printMessage("Creating " + numTerminals + " terminal(s) with " + (executionTimeMillis / 60000)
						+ " minute(s) of execution...");
			printMessage("Transaction Weights: " + (100 - (paymentWeight + orderStatusWeight + deliveryWeight + stockLevelWeight))
					+ "% New-Order, " + paymentWeight + "% Payment, " + orderStatusWeight + "% Order-Status, " + deliveryWeight
					+ "% Delivery, " + stockLevelWeight + "% Stock-Level");

			terminals = new jTPCCTerminal[numTerminals];
			terminalNames = new String[numTerminals];
			terminalsStarted = numTerminals;
			try {
				int[][] usedTerminals = new int[numWarehouses][10];
				for (int i = 0; i < numWarehouses; i++)
					for (int j = 0; j < 10; j++)
						usedTerminals[i][j] = 0;

				for (int i = 0; i < numTerminals; i++) {
					int terminalWarehouseID;
					int terminalDistrictID;
					do {
						terminalWarehouseID = (int) randomNumber(1, numWarehouses);
            int distPerWhse = (int)(configDistPerWhse * factor);
            if(distPerWhse < 1) {distPerWhse = 1;}
						terminalDistrictID = (int) randomNumber(1, distPerWhse);
					} while (usedTerminals[terminalWarehouseID - 1][terminalDistrictID - 1] == 1);
					usedTerminals[terminalWarehouseID - 1][terminalDistrictID - 1] = 1;

					String terminalName = terminalPrefix + (i >= 9 ? "" + (i + 1) : "0" + (i + 1));
					Connection conn = null;
					printMessage("Creating database connection for " + terminalName + "...");
					conn = DriverManager.getConnection(database, username, password);
					conn.setAutoCommit(false);

					jTPCCTerminal terminal = new jTPCCTerminal(terminalName, terminalWarehouseID, terminalDistrictID, conn,
							transactionsPerTerminal, normalOutput, debugMessages, paymentWeight, orderStatusWeight, deliveryWeight,
							stockLevelWeight, numWarehouses, this, errorOutput, writeStandardOutToFile,factor);
					terminals[i] = terminal;
					terminalNames[i] = terminalName;
					printMessage(terminalName + "\t" + terminalWarehouseID);
				}

				sessionEndTargetTime = executionTimeMillis;
				signalTerminalsRequestEndSent = false;

				printMessage("\nTransaction\tWeight\n% New-Order\t"
						+ (100 - (paymentWeight + orderStatusWeight + deliveryWeight + stockLevelWeight)) + "\n% Payment\t" + paymentWeight
						+ "\n% Order-Status\t" + orderStatusWeight + "\n% Delivery\t" + deliveryWeight + "\n% Stock-Level\t"
						+ stockLevelWeight);
				printMessage("\n\nTransaction Number\tTerminal\tType\tExecution Time (ms)\t\tComment");

				printMessage("Created " + numTerminals + " terminal(s) successfully!");
			} catch (Exception e1) {
				e1.printStackTrace();
				printMessage("\nThis session ended with errors!");

				errorMessage("An error occurred!");
				StringWriter stringWriter = new StringWriter();
				PrintWriter printWriter = new PrintWriter(stringWriter);
				e1.printStackTrace(printWriter);
				printWriter.close();
				printMessage(stringWriter.toString());
				throw new Exception();
			}

			// Now ready to start transactions.

		}
	}

	public void startTransactions() {

		sessionStart = getCurrentTime();
		sessionStartTimestamp = System.currentTimeMillis();
		sessionNextTimestamp = sessionStartTimestamp;
		if (sessionEndTargetTime != -1)
			sessionEndTargetTime += sessionStartTimestamp;

		synchronized (terminals) {
			printMessage("Starting all terminals...");
			transactionCount = 1;
			for (int i = 0; i < terminals.length; i++)
				(new Thread(terminals[i])).start();
		}

		printMessage("All terminals started executing " + sessionStart);
	}

	public void stopTransactions() {
		signalTerminalsRequestEnd(false);
	}

	private void signalTerminalsRequestEnd(boolean timeTriggered) {
		synchronized (terminals) {
			if (!signalTerminalsRequestEndSent) {
				if (timeTriggered)
					printMessage("The time limit has been reached.");
				printMessage("Signalling all terminals to stop...");
				signalTerminalsRequestEndSent = true;

				for (int i = 0; i < terminals.length; i++)
					if (terminals[i] != null)
						terminals[i].stopRunningWhenPossible();

				printMessage("Waiting for all active transactions to end...");
			}
		}
	}

	public void signalTerminalEnded(jTPCCTerminal terminal, long countNewOrdersExecuted) {
		synchronized (terminals) {
			boolean found = false;
			terminalsStarted--;
			for (int i = 0; i < terminals.length && !found; i++) {
				if (terminals[i] == terminal) {
					terminals[i] = null;
					terminalNames[i] = "(" + terminalNames[i] + ")";

					newOrderCounter += countNewOrdersExecuted;
					found = true;
				}
			}
		}

		if (terminalsStarted == 0) {

			sessionEnd = getCurrentTime();
			sessionEndTimestamp = System.currentTimeMillis();
			sessionEndTargetTime = -1;
			printMessage("All terminals finished executing " + sessionEnd);
			endReport();
			if (thereWereErrors) {

				printMessage("There were errors on this session!");
			}
			printMessage("Session #" + sessionCount + " finished!");
			setTerminalsFinished(true);
		}
	}

	public void signalTerminalEndedTransaction(String terminalName, String transactionType, long executionTime, String comment, int newOrder) {
		if (comment == null)
			comment = "None";

		if (writeStandardOutToFile){
		try {
			synchronized (normalOutput) {
				printMessage("" + transactionCount + "\t" + terminalName + "\t" + transactionType + "\t" + executionTime + "\t\t" + comment);
				transactionCount++;
				executedTransactionCounter += newOrder;
			}
		} catch (Exception e) {
			errorMessage("An error occurred writing the report!");
		}
		}

		if (sessionEndTargetTime != -1 && System.currentTimeMillis() > sessionEndTargetTime) {
			signalTerminalsRequestEnd(true);
		}

		provideStatusReport();
	}

	private void endReport() {
		printMessage("\n\nMeasured tpmC\t=60000*" + newOrderCounter + "/" + (sessionEndTimestamp - sessionStartTimestamp));
		printMessage("\nSession Start\t" + sessionStart + "\nSession End\t" + sessionEnd);
		printMessage("Transaction Count\t" + (transactionCount - 1));

	}

	private void printMessage(String message) {
		if (OUTPUT_MESSAGES && writeStandardOutToFile)
			try {
				normalOutput.write(message.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

	private void errorMessage(String message) {
		thereWereErrors = true;
		try {
			System.err.println(message);
			errorOutput.write(message.getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void removeAllTerminals() {
		terminals = null;
		System.gc();
	}

	private String provideStatusReport() {
		String informativeText = "";
		long currTimeMillis = System.currentTimeMillis();

		if (executedTransactionCounter != 0) {
			double tpmC = (6000000 * executedTransactionCounter / (currTimeMillis - sessionStartTimestamp)) / 100.0;
			informativeText = "Running Average tpmC: " + tpmC + "      ";
		}

		if (currTimeMillis > sessionNextTimestamp) {
			sessionNextTimestamp += 5000; /* check this every 5 seconds */
			recentTpmC = (executedTransactionCounter - sessionNextKounter) * 12;
			sessionNextKounter = executedTransactionCounter;
		}

		if (executedTransactionCounter != 0) {
			informativeText += "Current tpmC: " + recentTpmC + "     ";
		}

		long freeMem = Runtime.getRuntime().freeMemory() / (1024 * 1024);
		long totalMem = Runtime.getRuntime().totalMemory() / (1024 * 1024);
		informativeText += "Memory Usage: " + (totalMem - freeMem) + "MB / " + totalMem + "MB";

		informativeText += "\n";
		try {
			summaryOutput.write(informativeText.getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}

		return informativeText;
	}

	private long randomNumber(long min, long max) {
		return (long) (random.nextDouble() * (max - min + 1) + min);
	}

	private String getCurrentTime() {
		return dateFormat.format(new java.util.Date());
	}

	public synchronized void setTerminalsFinished(boolean terminals_finished) {
		this.terminalsFinished = terminals_finished;
	}

	public synchronized boolean areAllTerminalsFinished() {
		return terminalsFinished;
	}

	public void terminateProcess() {
		System.out.println("terminating process...");
		System.exit(1);
	}

}
