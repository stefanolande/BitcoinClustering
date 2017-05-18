package are2.visualizer;

import java.io.*;

/**
 * Created by lande on 17/05/2017.
 */
public class Visualizer {
    public static void main(String[] args) {
        StringBuilder page = new StringBuilder();

        page.append("<html>\n" +
                "<head>\n" +
                "    <script type=\"text/javascript\" src=\"vis.js\"></script>\n" +
                "    <link href=\"vis.css\" rel=\"stylesheet\" type=\"text/css\" />\n" +
                "\n" +
                "    <style type=\"text/css\">\n" +
                "        #mynetwork {\n" +
                "            border: 1px solid lightgray;\n" +
                "        }\n" +
                "    </style>\n" +
                "</head>\n" +
                "<body>\n" +
                "<div id=\"mynetwork\"></div>\n" +
                "\n" +
                "<script type=\"text/javascript\">\n");

        StringBuilder nodes = new StringBuilder();
        nodes.append("var nodes = new vis.DataSet([\n");

        StringBuilder edges = new StringBuilder();
        edges.append(" var edges = new vis.DataSet([\n");

        StringBuilder cIds = new StringBuilder();
        cIds.append(" var cIds = [");

        try (BufferedReader reader = new BufferedReader(new FileReader("clustersOnlyTagged.txt"))) {
            String line;

            String lastCluster = "";
            int id = 1;
            int clusterCounter = 1;
            int lastClusrNodeId = 1;

            while ((line = reader.readLine()) != null) {
                String[] tokens = line.replace("(", "")
                        .replace(")", "")
                        .split(",");

                String address = tokens[0];
                String clusterId = tokens[1];

                String tag;
                try {
                    tag = "\\n " + tokens[2];
                } catch (ArrayIndexOutOfBoundsException e) {
                    tag = "";
                }

                if (!lastCluster.equals(clusterId)) {
                    lastCluster = clusterId;

                    lastClusrNodeId = id;
                    nodes.append("{id: " + (id++) + ", label: 'Cluster " +
                            (clusterCounter++) + "' , color: '#2d7ce7', " +
                            "cid: " + lastClusrNodeId +"},\n");

                    cIds.append("'" + lastClusrNodeId + "', ");
                }


                nodes.append("{id: " + (id++) + ", label: '" +
                        address.substring(0, 7) + tag + "', title: '" + address + "', " +
                        "cid: " + lastClusrNodeId +"},\n");


                edges.append("{from: " + lastClusrNodeId + ", to: " + (id - 1) + "},\n");

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        nodes.append("]);\n");
        edges.append("]);\n");
        cIds.append("];\n");

        page.append(nodes);
        page.append(edges);
        page.append(cIds);
        page.append("    var container = document.getElementById('mynetwork');\n" +
                "    var data = {\n" +
                "        nodes: nodes,\n" +
                "        edges: edges\n" +
                "    };\n" +
                "    var options = {\n" +
                "      nodes: {\n" +
                "        shape: 'circle'\n" +
                "    \t},\n" +
                "   \t   physics: {\n" +
                "        barnesHut: {\n" +
                "          gravitationalConstant: -5000,\n" +
                "          centralGravity: 1,\n" +
                "          damping : 0.3\n" +
                "        },\n" +
                "      stabilization: {\n" +
                "      enabled: true,\n" +
                "      iterations: 100,\n" +
                "      updateInterval: 100,\n" +
                "      onlyDynamicEdges: false,\n" +
                "      fit: true,\n" +
                "    },\n" +
                "    timestep: 0.35\n" +
                "\t}\n" +
                "}    \n" +
                "var network = new vis.Network(container, data, options);\n" +
                "cIds.forEach(function(id) {\n" +
                        "    network.clusterByConnection(id)\n" +
                        "\n" +
                        "});" +
                "  network.on(\"selectNode\", function(params) {\n" +
                "      if (params.nodes.length == 1) {\n" +
                "          if (network.isCluster(params.nodes[0]) == true) {\n" +
                "              network.openCluster(params.nodes[0]);\n" +
                "} else {\n" +
                "          \tnetwork.clusterByConnection(params.nodes[0])\n" +
                "          }" +
                "      }\n" +
                "  });" +
                "</script>\n" +
                "</body>\n" +
                "</html>");

        try (BufferedWriter bw = new BufferedWriter(new FileWriter("index.html"))) {
            bw.write(page.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
