package are2.visualizer;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lande on 17/05/2017.
 */
public class Visualizer {
    public static void main(String[] args) {
        StringBuilder page = new StringBuilder();

        Map<Integer, Integer> nodesInCluster = new HashMap<>();

        page.append("<html>\n" +
                "<head>\n" +
                "\t<script type=\"text/javascript\" src=\"vis.js\"></script>\n" +
                "\t<link href=\"vis.css\" rel=\"stylesheet\" type=\"text/css\" />\n" +
                "\n" +
                "\t<style type=\"text/css\">\n" +
                "\t\t#mynetwork {\n" +
                "\t\t\tborder: 1px solid lightgray;\n" +
                "\t\t}\n" +
                "\t</style>\n" +
                "</head>\n" +
                "<body>\n" +
                "\t<div id=\"mynetwork\"></div>\n" +
                "\n" +
                "\t<script type=\"text/javascript\">\n");

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
                            "cid: " + lastClusrNodeId + ", font: {color: 'white'}},\n");

                    cIds.append("'" + lastClusrNodeId + "', ");
                }

                nodesInCluster.merge(lastClusrNodeId, 1, Integer::sum);

                nodes.append("{id: " + (id++) + ", label: '" +
                        address.substring(0, 7) + tag + "', title: '" + address + "', " +
                        "cid: " + lastClusrNodeId +
                        (!tag.equals("") ? ", color: '#FF8141'" : "") +
                        "},\n");


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
        page.append("var container = document.getElementById('mynetwork');\n" +
                "var data = {\n" +
                "\tnodes: nodes,\n" +
                "\tedges: edges\n" +
                "};\n" +
                "var options = {\n" +
                "\tnodes: {\n" +
                "\t\tshape: 'circle'\n" +
                "\t},\n" +
                "\tphysics: {\n" +
                "\t\tbarnesHut: {\n" +
                "\t\t\tgravitationalConstant: -10000,\n" +
                "\t\t\tcentralGravity: 0.1,\n" +
                "\t\t\tdamping : 0.3\n" +
                "\t\t},\n" +
                "\t\tstabilization: {\n" +
                "\t\t\tenabled: true,\n" +
                "\t\t\titerations: 50,\n" +
                "\t\t\tupdateInterval: 100,\n" +
                "\t\t\tonlyDynamicEdges: false,\n" +
                "\t\t\tfit: true,\n" +
                "\t\t},\n" +
                "\t\ttimestep: 0.35\n" +
                "\t}\n" +
                "}    \n" +
                "var network = new vis.Network(container, data, options);");

        page.append("var map = {");
        for (int clusterId : nodesInCluster.keySet()) {
            page.append(" \"" + clusterId + "\": \"" + nodesInCluster.get(clusterId) + "\",");
        }
        page.append("};");

        page.append("cIds.forEach(function(id) {\n" +
                "\n" +
                "\tvar clusterOptions = {\n" +
                "\t\tclusterNodeProperties: {id: 'cluster'+id, label: 'Cluster \\n size '+map[id], borderWidth:3, color: '#2d7ce7', \n" +
                "\t\tfont: {size: 25 * Math.ceil(Math.log10(map[id])), color: 'white'} }          \n" +
                "\t};\n" +
                "\tnetwork.clusterByConnection(id, clusterOptions)\n" +
                "\n" +
                "});  \n" +
                "network.on(\"selectNode\", function(params) {\n" +
                "\tvar id = params.nodes[0]\n" +
                "\tif (params.nodes.length == 1) {\n" +
                "\t\tif (network.isCluster(id) == true) {\n" +
                "\t\t\tnetwork.openCluster(id);\n" +
                "\t\t} else {\t\n" +
                "\t\t\tvar clusterOptions = {\n" +
                "\t\t\t\tclusterNodeProperties: {id: 'cluster'+id, label: 'Cluster \\n size '+map[id], borderWidth:3, color: '#2d7ce7', \n" +
                "\t\tfont: {size: 25 * Math.ceil(Math.log10(map[id])), color: 'white'} } \n" +
                "\t\t\t};\n" +
                "\t\t\tnetwork.clusterByConnection(id, clusterOptions)\n" +
                "\t\t}      \n" +
                "\t}\n" +
                "});\n" +
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
