use crate::redisgears_plugin_api::GearsApiError;

type RedisAIOnDoneCallback = Box<dyn FnOnce(Result<Vec<Box<dyn AITensorInterface + Send>>, GearsApiError>)>;

pub trait AITensorInterface {
    fn get_data(&self) -> &[u8];
    fn dims(&self) -> Vec<i64>;
    fn element_size(&self) -> usize;
}

pub trait AIModelRunnerInterface {
    fn add_input(
        &mut self,
        name: &str,
        tensor: &dyn AITensorInterface,
    ) -> Result<(), GearsApiError>;
    fn add_output(&mut self, name: &str) -> Result<(), GearsApiError>;
    fn run(
        &mut self,
        on_done: RedisAIOnDoneCallback,
    );
}

pub trait AIScriptRunnerInterface {
    fn add_input(&mut self, tensor: &dyn AITensorInterface) -> Result<(), GearsApiError>;
    fn add_output(&mut self) -> Result<(), GearsApiError>;
    fn run(
        &mut self,
        on_done: RedisAIOnDoneCallback,
    );
}

pub trait AIModelInterface {
    fn get_model_runner(&self) -> Box<dyn AIModelRunnerInterface>;
}

pub trait AIScriptInterface {
    fn get_script_runner(&self, func_name: &str) -> Box<dyn AIScriptRunnerInterface>;
}
