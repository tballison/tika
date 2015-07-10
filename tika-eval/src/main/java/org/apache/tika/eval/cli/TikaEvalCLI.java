package org.apache.tika.eval.cli;

public class TikaEvalCLI {
    static final String[] tools = {"Profile", "Compare", "Report", "StartDB"};

    private static String specifyTools() {
        StringBuilder sb = new StringBuilder();
        sb.append("Must specify one of the following tools in the first parameter:\n");
        for (String s : tools) {
            sb.append(s+"\n");
        }
        return sb.toString();

    }

    private void execute(String[] args) {
        String tool = args[0];
        String[] subsetArgs = new String[args.length-1];
        System.arraycopy(args, 1, subsetArgs, 0, args.length - 1);
        if (tool.equals("Report")) {
            handleReport(subsetArgs);
        } else if (tool.equals("Compare")) {
            handleCompare(subsetArgs);
        } else if (tool.equals("Profile")) {
            handleProfile(subsetArgs);
        } else if (tool.equals("StartDB")) {
            handleStartDB(subsetArgs);
        } else {
            throw new RuntimeException(specifyTools());
        }
    }

    private void handleStartDB(String[] subsetArgs) {

    }

    private void handleProfile(String[] subsetArgs) {

    }

    private void handleCompare(String[] subsetArgs) {

    }

    private void handleReport(String[] subsetArgs) {
    }

    public static void main(String[] args) throws Exception {
        TikaEvalCLI cli = new TikaEvalCLI();
        if (args.length == 0) {
            throw new RuntimeException(specifyTools());
        }
        cli.execute(args);

    }
}
