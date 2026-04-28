package com.example.batchprocessing;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Кладёт URI текущего HTTP-запроса в MDC под ключом "uri",
 * чтобы он попал в JSON-логи рядом с traceId / spanId.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class MdcUriFilter extends OncePerRequestFilter {

    private static final String MDC_URI = "uri";

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        try {
            MDC.put(MDC_URI, request.getMethod() + " " + request.getRequestURI());
            filterChain.doFilter(request, response);
        } finally {
            MDC.remove(MDC_URI);
        }
    }
}