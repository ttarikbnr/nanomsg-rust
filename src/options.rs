use std::time::Duration;

#[derive(Clone)]
pub struct SocketOptions {
    auto_reconnect              : bool,
    reconnect_try_interval      : Duration,
    max_reconnection_try        : Option<usize>,
    connect_timeout             : Duration,
    tcp_nodelay                 : bool,
    tcp_linger                  : Option<Duration>,
}

impl Default for SocketOptions {
    fn default() -> Self {
        Self {
            auto_reconnect              : true,
            reconnect_try_interval      : Duration::from_secs(4),
            max_reconnection_try        : None,
            connect_timeout             : Duration::from_secs(4),
            tcp_nodelay                 : false,
            tcp_linger                  : None,
        }
    }
}

impl SocketOptions {
    pub fn get_auto_reconnect(&self) -> bool {
        self.auto_reconnect
    }

    pub fn set_auto_reconnect(&mut self, auto_reconnect: bool) {
        self.auto_reconnect = auto_reconnect;
    }

    pub fn get_reconnect_try_interval(&self) -> Duration {
        self.reconnect_try_interval
    }

    pub fn set_reconnect_try_interval(&mut self,
                                      reconnect_try_interval: Duration) {
        self.reconnect_try_interval = reconnect_try_interval; 
    }

    pub fn get_max_reconnection_try(&self) -> Option<usize> {
        self.max_reconnection_try
    }

    pub fn set_max_reconnection_try(&mut self,
                                    max_reconnection_try: Option<usize>) {
        self.max_reconnection_try = max_reconnection_try;
    }

    pub fn get_connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    pub fn set_connect_timeout(&mut self, connect_timeout  : Duration) {
        self.connect_timeout = connect_timeout;
    }

    pub fn get_tcp_nodelay(&self) -> bool {
        self.tcp_nodelay
    }

    pub fn set_tcp_nodelay(&mut self, tcp_nodelay : bool) {
        self.tcp_nodelay = tcp_nodelay;
    }

    pub fn get_tcp_linger(&self) -> Option<Duration> {
        self.tcp_linger
    }

    pub fn set_tcp_linger(&mut self, tcp_linger: Option<Duration>) {
        self.tcp_linger = tcp_linger;
    }
}