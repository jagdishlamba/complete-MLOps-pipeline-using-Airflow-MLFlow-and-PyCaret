# Complete MLOps Pipeline using Airflow, MLFlow, and PyCaret

This project demonstrates the implementation of a complete MLOps (Machine Learning Operations) pipeline using Airflow, MLFlow, and PyCaret. The goal of the project is to showcase the end-to-end process of developing, deploying, and managing machine learning models using popular open source tools in the MLOps ecosystem.

# MLOps Lead Scoring Assignment

## Problem statement
- CodePro is an EdTech startup that had a phenomenal seed A funding round. It used the money to increase its brand awareness. As the marketing spend increased, it got several leads from different sources. Although it had spent significant money on acquiring customers, it had to be profitable in the long run to sustain the business. The major cost that the company is incurring is the customer acquisition cost (CAC).
- At the initial stage, customer acquisition cost is required to be high in companies. But as their businesses grow, these companies start focussing on profitability. Many companies first offer their services for free or provide offers at the initial stages but later start charging customers for these services.
- Businesses want to reduce their customer acquisition costs in the long run. The high customer acquisition cost is due to following reasons:
    * Improper targeting
    * High competition
    * Inefficient conversion
- Job of ML team is to create Lead categorisation system to help sales team.


## Project Structure

The project is organized into two main folders:

1. **airflow**: This folder contains the implementation of three pipelines:
   - Data Pipeline: Responsible for data ingestion, preprocessing, and transformation.
   - Training Pipeline: Handles model training and hyperparameter tuning using PyCaret.
   - Inference Pipeline: Deploys the trained model for making predictions on new data.

2. **assignment**: This folder contains practice code that prepares the foundation for building the Airflow pipelines.

## Usage

Follow the steps below to set up and run the MLOps pipeline:

1. Clone the repository:

2. Install dependencies:

3. Set up your environment variables:

4. Airflow Setup:
- Install Apache Airflow following the official documentation.
- Configure Airflow to use the provided DAGs from the `airflow` folder.

5. Practice Code (Assignment Folder):
- Explore the practice code in the `assignment` folder to understand how to structure your pipelines.

6. Airflow Pipelines (Airflow Folder):
- Study the DAGs in the `airflow` folder to understand the orchestration of data pipeline, training pipeline, and inference pipeline.

7. MLFlow Integration:
- Integrate MLFlow into your training pipeline to track experiments and manage models.

8. PyCaret Usage:
- Utilize PyCaret in the training pipeline for efficient model selection and hyperparameter tuning.

9. Inference Deployment:
- Set up the inference pipeline to deploy your trained model for making predictions.

## License

This project is licensed under the [MIT License](LICENSE), meaning you are free to use, modify, and distribute the code for your purposes.

## Acknowledgments

We would like to express our gratitude to the open-source community for providing the tools and resources that made this project possible.
