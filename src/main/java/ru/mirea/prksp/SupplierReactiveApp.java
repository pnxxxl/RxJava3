package ru.mirea.prksp;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;

import java.io.Console;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class SupplierReactiveApp {
    private SupplierReactiveApp() {
    }

    public static void main(String[] args) {
        configureOutputEncoding();
        System.out.println("=== Обработка поставок ===");

        List<SupplierOffer> initial = List.of(
                offer(1, "Альфа", "Молоко", "10.00", 5),
                offer(2, "Бета", "Хлеб", "20.00", 2),
                offer(1, "Альфа", "Яйца", "8.00", 0),
                offer(2, "Бета", "Испорченная запись", "-5.00", 3),
                offer(1, "Альфа", "Сыр", "15.00", 3));

        // Новые элементы приходят после начального набора и сразу проходят ту же цепочку.
        List<SupplierOffer> later = List.of(
                offer(1, "Альфа", "Молоко", "10.00", 2),
                offer(2, "Бета", "Хлеб", "20.00", 1),
                offer(3, "Гамма", "Яблоки", "7.50", 4));

        Flowable<SupplierOffer> source = Flowable.fromIterable(initial)
                .concatWith(Flowable.interval(250, 250, TimeUnit.MILLISECONDS, Schedulers.single())
                        .take(later.size())
                        .map(index -> later.get(index.intValue())))
                .doOnNext(offer -> System.out.printf("[источник: %s] %s, %s, шт.: %d%n",
                        Thread.currentThread().getName(), offer.supplierName(),
                        offer.product(), offer.quantity()))
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.computation())
                .doOnNext(offer -> System.out.printf("[обработка: %s] %s%n",
                        Thread.currentThread().getName(), offer.product()));

        SupplierPipeline.totals(source,
                        error -> System.out.printf("[ошибка данных] %s%n", error.getMessage()))
                .blockingSubscribe(
                        total -> System.out.printf("[итог: %s] %s (id=%d): %d шт., сумма %s%n",
                                Thread.currentThread().getName(), total.supplierName(),
                                total.supplierId(), total.availableQuantity(),
                                total.totalCost().toPlainString()),
                        error -> System.err.printf("[ошибка потока] %s%n", error));

        if (List.of(args).contains("--backpressure")) {
            demonstrateBackpressure();
        }
    }

    private static SupplierOffer offer(int id, String supplier, String product, String price, int quantity) {
        return new SupplierOffer(id, supplier, product, new BigDecimal(price), quantity);
    }

    private static void configureOutputEncoding() {
        Console console = System.console();
        Charset charset = console == null ? StandardCharsets.UTF_8 : console.charset();
        System.setOut(new PrintStream(System.out, true, charset));
        System.setErr(new PrintStream(System.err, true, charset));
    }

    /** Быстрый источник уступает темп медленному обработчику через request(n). */
    private static void demonstrateBackpressure() {
        System.out.println("=== Backpressure: prefetch=4, обработчик 15 мс/элемент ===");
        AtomicInteger produced = new AtomicInteger();
        AtomicInteger handled = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch finished = new CountDownLatch(1);

        Flowable.range(1, 40)
                .doOnNext(value -> {
                    int pending = produced.incrementAndGet() - handled.get();
                    maxInFlight.accumulateAndGet(pending, Math::max);
                    if (value == 1) {
                        System.out.printf("[быстрый источник: %s]%n", Thread.currentThread().getName());
                    }
                })
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.computation(), false, 4)
                .subscribe(value -> {
                    if (value == 1) {
                        System.out.printf("[медленный обработчик: %s]%n", Thread.currentThread().getName());
                    }
                    TimeUnit.MILLISECONDS.sleep(15);
                    handled.incrementAndGet();
                }, error -> {
                    failure.set(error);
                    finished.countDown();
                }, finished::countDown);

        try {
            if (!finished.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Не дождались завершения демонстрации backpressure");
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Ожидание обработки прервано", error);
        }
        if (failure.get() != null) {
            throw new IllegalStateException("Ошибка демонстрации backpressure", failure.get());
        }

        System.out.printf("Создано: %d, обработано: %d, максимум ожидающих: %d%n",
                produced.get(), handled.get(), maxInFlight.get());
    }
}
