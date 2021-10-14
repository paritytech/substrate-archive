FROM docker.io/paritytech/ci-linux:production as builder
LABEL description="build stage for Substrate Archive. We are creating the binary."

ARG PROFILE=release
WORKDIR /bin/polkadot-archive/

COPY . /bin/polkadot-archive/

RUN cargo build --$PROFILE

# ===== SECOND STAGE ======

FROM docker.io/library/ubuntu:20.04
LABEL description="This is the 2nd stage: a very small image where we copy the Polkadot binary."
ARG PROFILE=release
COPY --from=builder /bin/polkadot-archive/target/$PROFILE/polkadot-archive /usr/local/bin

RUN useradd -m -u 1000 -U -s /bin/sh -d /polkadot-archive polkadot-archive && \
	mkdir -p /polkadot-archive/.local/share && \
	mkdir /data && \
	chown -R polkadot-archive:polkadot-archive /data && \
	ln -s /data /polkadot-archive/.local/share/polkadot-archive && \
	rm -rf /usr/bin /usr/sbin

USER polkadot-archive
VOLUME ["/data"]

CMD ["/usr/local/bin/polkadot-archive"]
