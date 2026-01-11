# Contributing to NPS BARB Data Pipeline

Thanks for your interest in contributing! This project is open source and welcomes contributions.

## How to Contribute

### Reporting Issues

If you find a bug or have a feature request:

1. Check if the issue already exists in [GitHub Issues](https://github.com/joshuafortunatus/nps_barb_data_pipeline/issues)
2. If not, create a new issue with:
   - Clear title and description
   - Steps to reproduce (for bugs)
   - Expected vs actual behavior
   - Any relevant logs or screenshots

### Submitting Changes

1. **Fork the repository**
2. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Make your changes**
   - Write clean, readable code
   - Add comments where necessary
   - Follow existing code style
4. **Test your changes locally**
   ```bash
   python scripts/fetch_nps_data.py
   python scripts/fetch_recreation_gov_data.py
   python scripts/rate_hikes.py
   ```
5. **Commit with clear messages**
   ```bash
   git commit -m "Add feature: description of what you added"
   ```
6. **Push to your fork**
   ```bash
   git push origin feature/your-feature-name
   ```
7. **Create a Pull Request**
   - Describe what your PR does
   - Reference any related issues

## Development Setup

1. Clone the repo:
   ```bash
   git clone https://github.com/joshuafortunatus/nps_barb_data_pipeline.git
   cd nps_barb_data_pipeline
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up your `.env` file with required API keys (see README)

4. Test the scripts locally before submitting

## Code Style

- Use descriptive variable names
- Add docstrings to functions
- Keep functions focused and single-purpose
- Follow PEP 8 style guidelines

## Ideas for Contributions

- Add support for more Recreation.gov facilities
- Implement error handling improvements
- Add data validation checks
- Create additional AI rating categories
- Optimize API request performance
- Add unit tests
- Improve documentation

## Questions?

Open an issue or reach out - happy to help! 🚀
