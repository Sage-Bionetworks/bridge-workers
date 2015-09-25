package org.sagebionetworks.bridge.workers;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.event.ContextClosedEvent;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

public final class App {

    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {

        final AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                ctx.close();
            }
        }));
        ctx.register(BridgeWorkersConfig.class);
        ctx.refresh();

        @SuppressWarnings("unchecked")
        List<Worker> workers = ctx.getBean("replicaWorkers", List.class);
        workers.forEach(worker -> {
            ctx.addApplicationListener(new ApplicationListener<ContextClosedEvent>() {
                @Override
                public void onApplicationEvent(ContextClosedEvent event) {
                    worker.shutdown();
                }
            });
            worker.run();
        });
        LOG.info("Relipca workers started.");
    }

    private App() {
    }
}
