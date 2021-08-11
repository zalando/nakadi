package org.zalando.nakadi.filters;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncListener;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

class AsyncContextWrapper implements AsyncContext {
    protected final AsyncContext context;

    AsyncContextWrapper(final AsyncContext context) {
        this.context = context;
    }

    @Override
    public void addListener(final AsyncListener listener) {
        this.context.addListener(listener);
    }

    @Override
    public void addListener(final AsyncListener listener, final ServletRequest servletRequest,
                            final ServletResponse servletResponse) {
        this.context.addListener(listener, servletRequest, servletResponse);
    }

    @Override
    public void complete() {
        this.context.complete();
    }

    @Override
    public <T extends AsyncListener> T createListener(final Class<T> clazz) throws ServletException {
        return this.context.createListener(clazz);
    }

    @Override
    public void dispatch() {
        this.context.dispatch();
    }

    @Override
    public void dispatch(final ServletContext context, final String path) {
        this.context.dispatch(context, path);
    }

    @Override
    public void dispatch(final String path) {
        this.context.dispatch(path);
    }

    @Override
    public ServletRequest getRequest() {
        return this.context.getRequest();
    }

    @Override
    public ServletResponse getResponse() {
        return this.context.getResponse();
    }

    @Override
    public long getTimeout() {
        return this.context.getTimeout();
    }

    @Override
    public boolean hasOriginalRequestAndResponse() {
        return this.context.hasOriginalRequestAndResponse();
    }

    @Override
    public void setTimeout(final long timeout) {
        this.context.setTimeout(timeout);
    }

    @Override
    public void start(final Runnable runnable) {
        this.context.start(runnable);
    }
}
