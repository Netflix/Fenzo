package com.netflix.fenzo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.fenzo.queues.QAttributes;
import com.netflix.fenzo.queues.QueuableTask;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * A class that provides a sample of large number of tasks to initialize TaskSchedulingService with that caused problems
 * in an actual run.
 */
public class SampleLargeNumTasksToInit {

    static class Task {
        private final String id;
        private final String bucket;
        private final int tier;
        private final double cpu;
        private final double memory;
        private final double networkMbps;
        private final double disk;
        private final String host;

        @JsonCreator
        public Task(@JsonProperty("id") String id,
                    @JsonProperty("bucket") String bucket,
                    @JsonProperty("tier") int tier,
                    @JsonProperty("cpu") double cpu,
                    @JsonProperty("memory") double memory,
                    @JsonProperty("networkMbps") double networkMbps,
                    @JsonProperty("disk") double disk,
                    @JsonProperty("host") String host) {
            this.id = id;
            this.bucket = bucket;
            this.tier = tier;
            this.cpu = cpu;
            this.memory = memory;
            this.networkMbps = networkMbps;
            this.disk = disk;
            this.host = host;
        }

        public String getId() {
            return id;
        }

        public String getBucket() {
            return bucket;
        }

        public int getTier() {
            return tier;
        }

        public double getCpu() {
            return cpu;
        }

        public double getMemory() {
            return memory;
        }

        public double getNetworkMbps() {
            return networkMbps;
        }

        public double getDisk() {
            return disk;
        }

        public String getHost() {
            return host;
        }
    }

    static List<Task> getSampleTasksInRunningState() throws IOException {
        final InputStream inputStream = SampleLargeNumTasksToInit.class.getClassLoader().getResourceAsStream("largeFenzoTasksInput.json");
        byte[] buf = new byte[1024];
        int n=0;
        StringBuilder json = new StringBuilder();
        while (n >= 0) {
            n = inputStream.read(buf, 0, 1024);
            if (n>0)
                json.append(new String(buf, 0, n));
        }
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(json.toString(), new TypeReference<List<Task>>() {});
    }

    static QueuableTask toQueuableTask(final Task t) {
        final QAttributes a = new QAttributes.QAttributesAdaptor(0, t.bucket);
        return new QueuableTask() {
            @Override
            public QAttributes getQAttributes() {
                return a;
            }

            @Override
            public String getId() {
                return t.id;
            }

            @Override
            public String taskGroupName() {
                return null;
            }

            @Override
            public double getCPUs() {
                return t.cpu;
            }

            @Override
            public double getMemory() {
                return t.memory;
            }

            @Override
            public double getNetworkMbps() {
                return t.networkMbps;
            }

            @Override
            public double getDisk() {
                return t.disk;
            }

            @Override
            public int getPorts() {
                return 0;
            }

            @Override
            public Map<String, Double> getScalarRequests() {
                return null;
            }

            @Override
            public Map<String, NamedResourceSetRequest> getCustomNamedResources() {
                return null;
            }

            @Override
            public List<? extends ConstraintEvaluator> getHardConstraints() {
                return null;
            }

            @Override
            public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
                return null;
            }

            @Override
            public void setAssignedResources(AssignedResources assignedResources) {

            }

            @Override
            public AssignedResources getAssignedResources() {
                return null;
            }
        };
    }
}
