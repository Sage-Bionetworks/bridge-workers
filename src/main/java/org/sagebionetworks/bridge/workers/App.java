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

    @SuppressWarnings("unchecked")
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

        ctx.getBean("replicaWorkers", List.class).forEach(worker -> startWorker((Worker)worker, ctx));
        LOG.info("Relipca workers started.");

        startWorker(ctx.getBean("uploadStatusWorker", Worker.class), ctx);
        LOG.info("Upload status worker started.");
    }

    private static void startWorker(final Worker worker, final AnnotationConfigApplicationContext ctx) {
        ctx.addApplicationListener(new ApplicationListener<ContextClosedEvent>() {
            @Override
            public void onApplicationEvent(ContextClosedEvent event) {
                worker.shutdown();
            }
        });
        worker.run();
    }

    private App() {
    }
}
