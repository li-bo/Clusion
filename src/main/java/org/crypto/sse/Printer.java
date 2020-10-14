package org.crypto.sse;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mjq on 2020/9/29.
 */
public class Printer {
    public enum LEVEL {NONE, NORMAL, STATS, DEBUG, EXTRA}

    ;

    final private static List<Printer> PRINTERS = new ArrayList<Printer>();

    private LEVEL level = LEVEL.NORMAL;

    /**
     * Sets level to NORMAL by default.
     */
    public Printer() {
    }

    public Printer(LEVEL l) {
        level = l;
    }

    /**
     * Specifies how this printer prints. Override this method
     * to define a different printing mathod.
     *
     * @param output
     */
    protected void concretePrint(String output) {
        System.out.print(output);
    }

    /**
     * Closes any resources that a printer might have open. Override
     * this method as needed.
     */
    protected void concreteClose() {
    }


    /**
     * Printer that prints to a file.
     *
     * @author Ryan Estes
     */
    public static class FilePrinter extends Printer {

        private BufferedWriter file;

        public FilePrinter(String filename) {
            openFile(filename);
        }

        public FilePrinter(LEVEL l, String filename) {
            super(l);
            openFile(filename);
        }

        private void openFile(String filename) {
            try {
                file = new BufferedWriter(new FileWriter(filename, true));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void concretePrint(String output) {
            try {
                file.write(output);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void concreteClose() {
            try {
                file.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    private boolean canPrint(LEVEL l) {
        boolean result = false;
        switch (l) {
            case NORMAL:
                if (level == LEVEL.NORMAL) {
                    result = true;
                    break;
                }
            case STATS:
                if (level == LEVEL.STATS) {
                    result = true;
                    break;
                }
            case DEBUG:
                if (level == LEVEL.DEBUG) {
                    result = true;
                    break;
                }
            case EXTRA:
                if (level == LEVEL.EXTRA) {
                    result = true;
                    break;
                }
            default:
                break;
        }

        return result;
    }
    public void setLevel(LEVEL l) {
        level = l;
    }

    public static void print(String output, LEVEL l) {
        for (Printer printer : PRINTERS) {
            if (printer.canPrint(l)) {
                printer.concretePrint(output);
            }
        }
    }
    /**
     * Close all printers that need to be closed. Such as
     * {@link FilePrinter}
     */
    public static void close() {
        for (Printer printer : PRINTERS) {
            printer.concreteClose();
        }
    }

    /**
     * Calls {@link #print(String, LEVEL)} using
     * NORMAL level.
     *
     * @param output to print.
     */
    public static void normal(String output) {
        print(output, LEVEL.NORMAL);
    }

    /**
     * Calls {@link #println(String, LEVEL)} using
     * NORMAL level.
     *
     * @param output to print.
     */
    public static void normalln(String output) {
        print(output + "\n", LEVEL.NORMAL);
    }

    /**
     * Calls {@link #print(String, LEVEL)} using
     * STATS level.
     *
     * @param output to print.
     */
    public static void stats(String output) {
        print(output, LEVEL.STATS);
    }

    /**
     * Calls {@link #println(String, LEVEL)} using
     * STATS level.
     *
     * @param output to print.
     */
    public static void statsln(String output) {
        print(output + "\n", LEVEL.STATS);
    }

    /**
     * Calls {@link #print(String, LEVEL)} using
     * DEBUG level.
     *
     * @param output to print.
     */
    public static void debug(String output) {
        print(output, LEVEL.DEBUG);
    }

    /**
     * Calls {@link #println(String, LEVEL)} using
     * DEBUG level.
     *
     * @param output to print.
     */
    public static void debugln(String output) {
        print(output + "\n", LEVEL.DEBUG);
    }

    /**
     * Calls {@link #print(String, LEVEL)} using
     * EXTRA level.
     *
     * @param output to print.
     */
    public static void extra(String output) {
        print(output, LEVEL.EXTRA);
    }

    /**
     * Calls {@link #println(String, LEVEL)} using
     * EXTRA level.
     *
     * @param output to print.
     */
    public static void extraln(String output) {
        print(output + "\n", LEVEL.EXTRA);
    }


    /**
     * Print some output at some level with the newline
     * character at the end of output.
     *
     * @param output to print.
     * @param l level to print at.
     */
    public static void println(String output, LEVEL l) {
        print(output + "\n", l);
    }
    /**
     * Adds to the list of printers which will be used
     * when calling any of the print functions or
     * {@link #close()}
     *
     * @param printer to add
     */
    public static void addPrinter(Printer printer) {
        PRINTERS.add(printer);
    }
}
