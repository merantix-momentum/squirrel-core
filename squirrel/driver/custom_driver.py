
from squirrel.driver import IterDriver
from squirrel.iterstream import Composable

class CustomDriver(IterDriver):

    def __init__(
            self, 
            train_driver: Composable | None = None, 
            test_driver: Composable | None = None, 
            val_driver: Composable | None = None, 
            **kwargs
        ) -> None:
        # TODO: add docstring 
        super().__init__(**kwargs)
        self.train_driver = train_driver
        self.test_driver = test_driver
        self.val_driver = val_driver
    
    def get_iter(self, split: str = "train", **kwargs) -> Composable:
        # TODO: add docstring
        assert split in ["train", "test", "val"]
        if split == "train" and self.train_driver:
            return self.train_driver.get_iter()
        elif split == "test" and self.test_driver:
            return self.test_driver.get_iter()
        elif split == "val" and self.val_driver:
            return self.val_driver.get_iter()
        else:
            raise ValueError(f"Driver for split name {split} was not given")