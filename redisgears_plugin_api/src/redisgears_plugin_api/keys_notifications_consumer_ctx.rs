use crate::redisgears_plugin_api::run_function_ctx::BackgroundRunFunctionCtxInterface;
use crate::redisgears_plugin_api::run_function_ctx::RedisClientCtxInterface;
use std::any::Any;

pub trait NotificationFiredDataInterface {}

pub trait NotificationRunCtxInterface {
    fn get_redis_client(&self) -> Box<dyn RedisClientCtxInterface>;
    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface>;
}

pub trait KeysNotificationsConsumerCtxInterface {
    fn on_notification_fired(
        &self,
        event: &str,
        key: &str,
        notification_ctx: Box<dyn NotificationRunCtxInterface>,
    ) -> Option<Box<dyn Any>>;

    fn post_command_notification(
        &self,
        notificaion_data: Option<Box<dyn Any>>,
        notification_ctx: Box<dyn NotificationRunCtxInterface>,
        ack_callback: Box<dyn FnOnce(Result<(), String>) + Send + Sync>,
    );
}
